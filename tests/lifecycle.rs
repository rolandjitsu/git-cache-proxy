// SPDX-License-Identifier: Apache-2.0
//! Exercise real signals against the binary and a stalled upstream process group.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::process::Stdio;
use std::time::Duration;

use rustix::process::{Pid, Signal, kill_process};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

#[tokio::test]
async fn signals_cancel_upstream_before_http_drain() {
    for signal in [Signal::TERM, Signal::INT] {
        let root = tempfile::tempdir().unwrap();
        let ready = root.path().join("ready");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&ready)
                .status()
                .unwrap()
                .success()
        );
        let wrapper = root.path().join("git-wrapper");
        std::fs::write(
            &wrapper,
            "#!/bin/sh\nsleep 30 &\necho \"$$ $!\" > \"$READY_FIFO\"\nwait\n",
        )
        .unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let mut server = Command::new(env!("CARGO_BIN_EXE_git-cache-proxy"))
            .args([
                "--upstream",
                "https://example.com",
                "--bind",
                &address.to_string(),
                "--upstream-timeout-seconds",
                "10",
            ])
            .arg("--cache-root")
            .arg(root.path().join("cache"))
            .arg("--git-binary")
            .arg(&wrapper)
            .env("READY_FIFO", &ready)
            .env_remove("RUST_LOG")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(server.stdout.take().unwrap()).lines();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let line = lines
                    .next_line()
                    .await
                    .unwrap()
                    .expect("server exited before binding");
                if line.contains("listening on") {
                    break;
                }
            }
        })
        .await
        .expect("server startup");
        let request = tokio::spawn(reqwest::get(format!(
            "http://{address}/repo.git/info/refs?service=git-upload-pack"
        )));
        let pids = tokio::time::timeout(Duration::from_secs(5), tokio::fs::read_to_string(&ready))
            .await
            .expect("upstream started")
            .unwrap();
        kill_process(Pid::from_raw(server.id().unwrap() as i32).unwrap(), signal).unwrap();
        let status = tokio::time::timeout(Duration::from_secs(5), server.wait())
            .await
            .expect("shutdown must cancel upstream before drain")
            .unwrap();
        assert!(status.success(), "signal {signal:?}: {status}");
        let response = request.await.unwrap().unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_GATEWAY);
        let mut pids = pids.split_whitespace();
        let leader = pids.next().unwrap();
        assert!(!std::path::Path::new(&format!("/proc/{leader}")).exists());
        let descendant = pids.next().unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Ok(stat) = tokio::fs::read_to_string(format!("/proc/{descendant}/stat")).await
            {
                if stat.split_whitespace().nth(2) == Some("Z") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("descendant must stop after group kill");
    }
}
