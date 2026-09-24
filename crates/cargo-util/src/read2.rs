#[cfg(not(target_os = "scarlet"))]
pub use self::imp::read2;
#[cfg(target_os = "scarlet")]
pub use self::scarlet_imp::read2;

// Scarlet's Native process pipes are blocking and do not expose poll or
// O_NONBLOCK. Drain both pipes concurrently so a child that fills one pipe
// while writing the other cannot deadlock its parent.
#[cfg(any(target_os = "scarlet", test))]
mod scarlet_imp {
    use std::io::{self, Read};
    use std::process::{ChildStderr, ChildStdout};
    use std::sync::mpsc;
    use std::thread;

    enum Chunk {
        Bytes(bool, Vec<u8>),
        End(bool),
        Error(io::Error),
    }

    fn drain(mut pipe: impl Read, stdout: bool, sender: mpsc::SyncSender<Chunk>) {
        let mut buffer = [0u8; 8192];
        loop {
            match pipe.read(&mut buffer) {
                Ok(0) => {
                    let _ = sender.send(Chunk::End(stdout));
                    return;
                }
                Ok(count) => {
                    if sender
                        .send(Chunk::Bytes(stdout, buffer[..count].to_vec()))
                        .is_err()
                    {
                        return;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    let _ = sender.send(Chunk::Error(error));
                    return;
                }
            }
        }
    }

    pub fn read2(
        out_pipe: ChildStdout,
        err_pipe: ChildStderr,
        data: &mut dyn FnMut(bool, &mut Vec<u8>, bool),
    ) -> io::Result<()> {
        let (sender, receiver) = mpsc::sync_channel(8);
        thread::scope(|scope| {
            let out_sender = sender.clone();
            let out_reader = scope.spawn(move || drain(out_pipe, true, out_sender));
            let err_reader = scope.spawn(move || drain(err_pipe, false, sender));
            let mut out = Vec::new();
            let mut err = Vec::new();
            let mut ended = 0;
            let mut first_error = None;
            while ended != 2 {
                match receiver.recv() {
                    Ok(Chunk::Bytes(true, bytes)) => {
                        out.extend_from_slice(&bytes);
                        data(true, &mut out, false);
                    }
                    Ok(Chunk::Bytes(false, bytes)) => {
                        err.extend_from_slice(&bytes);
                        data(false, &mut err, false);
                    }
                    Ok(Chunk::End(stdout)) => {
                        ended += 1;
                        if stdout {
                            data(true, &mut out, true);
                        } else {
                            data(false, &mut err, true);
                        }
                    }
                    Ok(Chunk::Error(error)) => {
                        ended += 1;
                        first_error.get_or_insert(error);
                    }
                    Err(_) => return Err(io::ErrorKind::BrokenPipe.into()),
                }
            }
            out_reader.join().expect("stdout pipe reader panicked");
            err_reader.join().expect("stderr pipe reader panicked");
            first_error.map_or(Ok(()), Err)
        })
    }

    #[cfg(all(test, unix))]
    #[test]
    fn drains_both_blocking_pipes_with_streaming_callbacks() {
        use std::process::{Command, Stdio};
        let mut child = Command::new("sh")
            .args(["-c", "i=0; while [ $i -lt 4096 ]; do printf 'out0123456789'; printf 'err0123456789' >&2; i=$((i+1)); done"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut done = [false; 2];
        read2(
            child.stdout.take().unwrap(),
            child.stderr.take().unwrap(),
            &mut |out, buffer, end| {
                if out {
                    stdout.extend_from_slice(buffer);
                    done[0] = end;
                } else {
                    stderr.extend_from_slice(buffer);
                    done[1] = end;
                }
                buffer.clear();
            },
        )
        .unwrap();
        assert!(child.wait().unwrap().success());
        assert_eq!(stdout.len(), 4096 * b"out0123456789".len());
        assert_eq!(stderr.len(), 4096 * b"err0123456789".len());
        assert_eq!(done, [true, true]);
    }
}

#[cfg(unix)]
mod imp {
    use libc::{F_GETFL, F_SETFL, O_NONBLOCK, c_int, fcntl};
    use std::io;
    use std::io::prelude::*;
    use std::mem;
    use std::os::unix::prelude::*;
    use std::process::{ChildStderr, ChildStdout};

    fn set_nonblock(fd: c_int) -> io::Result<()> {
        let flags = unsafe { fcntl(fd, F_GETFL) };
        if flags == -1 || unsafe { fcntl(fd, F_SETFL, flags | O_NONBLOCK) } == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn read2(
        mut out_pipe: ChildStdout,
        mut err_pipe: ChildStderr,
        data: &mut dyn FnMut(bool, &mut Vec<u8>, bool),
    ) -> io::Result<()> {
        set_nonblock(out_pipe.as_raw_fd())?;
        set_nonblock(err_pipe.as_raw_fd())?;

        let mut out_done = false;
        let mut err_done = false;
        let mut out = Vec::new();
        let mut err = Vec::new();

        let mut fds: [libc::pollfd; 2] = unsafe { mem::zeroed() };
        fds[0].fd = out_pipe.as_raw_fd();
        fds[0].events = libc::POLLIN;
        fds[1].fd = err_pipe.as_raw_fd();
        fds[1].events = libc::POLLIN;
        let mut nfds = 2;
        let mut errfd = 1;

        while nfds > 0 {
            // wait for either pipe to become readable using `poll`
            let r = unsafe { libc::poll(fds.as_mut_ptr(), nfds, -1) };
            if r == -1 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(err);
            }

            // Read as much as we can from each pipe, ignoring EWOULDBLOCK or
            // EAGAIN. If we hit EOF, then this will happen because the underlying
            // reader will return Ok(0), in which case we'll see `Ok` ourselves. In
            // this case we flip the other fd back into blocking mode and read
            // whatever's leftover on that file descriptor.
            let handle = |res: io::Result<_>| match res {
                Ok(_) => Ok(true),
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        Ok(false)
                    } else {
                        Err(e)
                    }
                }
            };
            if !err_done && fds[errfd].revents != 0 && handle(err_pipe.read_to_end(&mut err))? {
                err_done = true;
                nfds -= 1;
            }
            data(false, &mut err, err_done);
            if !out_done && fds[0].revents != 0 && handle(out_pipe.read_to_end(&mut out))? {
                out_done = true;
                fds[0].fd = err_pipe.as_raw_fd();
                errfd = 0;
                nfds -= 1;
            }
            data(true, &mut out, out_done);
        }
        Ok(())
    }
}

#[cfg(windows)]
mod imp {
    use std::io;
    use std::os::windows::prelude::*;
    use std::process::{ChildStderr, ChildStdout};
    use std::slice;

    use miow::Overlapped;
    use miow::iocp::{CompletionPort, CompletionStatus};
    use miow::pipe::NamedPipe;
    use windows_sys::Win32::Foundation::ERROR_BROKEN_PIPE;

    struct Pipe<'a> {
        dst: &'a mut Vec<u8>,
        overlapped: Overlapped,
        pipe: NamedPipe,
        done: bool,
    }

    pub fn read2(
        out_pipe: ChildStdout,
        err_pipe: ChildStderr,
        data: &mut dyn FnMut(bool, &mut Vec<u8>, bool),
    ) -> io::Result<()> {
        let mut out = Vec::new();
        let mut err = Vec::new();

        let port = CompletionPort::new(1)?;
        port.add_handle(0, &out_pipe)?;
        port.add_handle(1, &err_pipe)?;

        unsafe {
            let mut out_pipe = Pipe::new(out_pipe, &mut out);
            let mut err_pipe = Pipe::new(err_pipe, &mut err);

            out_pipe.read()?;
            err_pipe.read()?;

            let mut status = [CompletionStatus::zero(), CompletionStatus::zero()];

            while !out_pipe.done || !err_pipe.done {
                for status in port.get_many(&mut status, None)? {
                    if status.token() == 0 {
                        out_pipe.complete(status);
                        data(true, out_pipe.dst, out_pipe.done);
                        out_pipe.read()?;
                    } else {
                        err_pipe.complete(status);
                        data(false, err_pipe.dst, err_pipe.done);
                        err_pipe.read()?;
                    }
                }
            }

            Ok(())
        }
    }

    impl<'a> Pipe<'a> {
        unsafe fn new<P: IntoRawHandle>(p: P, dst: &'a mut Vec<u8>) -> Pipe<'a> {
            // SAFETY: Handle must be owned, open, and closeable with CloseHandle.
            let pipe = unsafe { NamedPipe::from_raw_handle(p.into_raw_handle()) };
            Pipe {
                dst,
                pipe,
                overlapped: Overlapped::zero(),
                done: false,
            }
        }

        unsafe fn read(&mut self) -> io::Result<()> {
            let dst = unsafe { slice_to_end(self.dst) };
            // SAFETY: The buffer must be valid until the end of the I/O,
            // which is handled in `read2`.
            match unsafe { self.pipe.read_overlapped(dst, self.overlapped.raw()) } {
                Ok(_) => Ok(()),
                Err(e) => {
                    if e.raw_os_error() == Some(ERROR_BROKEN_PIPE as i32) {
                        self.done = true;
                        Ok(())
                    } else {
                        Err(e)
                    }
                }
            }
        }

        unsafe fn complete(&mut self, status: &CompletionStatus) {
            let prev = self.dst.len();
            unsafe { self.dst.set_len(prev + status.bytes_transferred() as usize) };
            if status.bytes_transferred() == 0 {
                self.done = true;
            }
        }
    }

    unsafe fn slice_to_end(v: &mut Vec<u8>) -> &mut [u8] {
        if v.capacity() == 0 {
            v.reserve(16);
        }
        if v.capacity() == v.len() {
            v.reserve(1);
        }
        unsafe { slice::from_raw_parts_mut(v.as_mut_ptr().add(v.len()), v.capacity() - v.len()) }
    }
}
