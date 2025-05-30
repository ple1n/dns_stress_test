use std::convert::Infallible;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use console::Term;
use dialoguer::Confirm;
use futures::executor::LocalPool;
use futures::stream::FuturesUnordered;
use futures::task::{LocalSpawnExt, SpawnExt};
use hickory_resolver::config::*;
use hickory_resolver::name_server::TokioConnectionProvider;
use hickory_resolver::proto::rr::RecordType;
use hickory_resolver::system_conf::read_system_conf;
use hickory_resolver::{Resolver, TokioResolver};
use tokio::task::JoinSet;
use tokio::time::Instant;

use futures;

#[tokio::main]
async fn main() {
    let (conf, mut opts) = read_system_conf().unwrap();
    opts.attempts = 1;
    opts.timeout = Duration::from_secs(5);
    println!("{:?} \n {:?}", &conf, &opts);
    if !Confirm::new().interact().unwrap() {
        return;
    }
    let mut rx = TokioResolver::builder_tokio().unwrap().with_options(opts);
    let opts = rx.options_mut();
    opts.cache_size = 0;
    let rx: Arc<
        Resolver<
            hickory_resolver::name_server::GenericConnector<
                hickory_resolver::proto::runtime::TokioRuntimeProvider,
            >,
        >,
    > = Arc::new(rx.build());
    let t = Term::stdout();
    // find max concurrency

    for n in [1e1, 1e2, 1e3, 1e4, 1e5, 1e6] {
        if bench(n as usize, rx.clone(), t.clone()).await.unwrap() > 0 {
            println!("max concurrency <= {}", n);
            break;
        }
    }
}

async fn bench(
    n: usize,
    rx: Arc<
        Resolver<
            hickory_resolver::name_server::GenericConnector<
                hickory_resolver::proto::runtime::TokioRuntimeProvider,
            >,
        >,
    >,
    term: Term,
) -> Result<usize, Box<dyn Error>> {
    let mut set = JoinSet::new();
    term.write_line("begin generating domains")?;
    let t = Instant::now();
    for _ in 0..n {
        let rx = rx.clone();
        let t = async move { rx.lookup(rand_domain(), RecordType::A).await };
        set.spawn(t);
    }
    term.clear_line()?;
    term.write_line(&format!(
        "generated {} domains, took {:?}, waiting for results",
        n,
        t.elapsed()
    ))?;
    let mut n_err = 0;
    loop {
        let rx = set.join_next().await;
        if let Some(rx) = rx {
            let rx = rx?;
            match rx {
                Ok(lp) => {}
                Err(er) => {
                    let s = format!("resolve error {}", er);
                    n_err += 1;
                }
            }
        } else {
            break;
        }
    }
    let took = t.elapsed();
    let avg = took / n as u32;
    term.write_line(&format!(
        "finished querying {:?} avg {:?}, err {}, total {}",
        took, avg, n_err, n
    ))?;

    Ok(n_err)
}

fn rand_domain() -> String {
    use rand;
    let n: u32 = rand::random();
    format!("{}.com", n)
}
