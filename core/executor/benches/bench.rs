mod bench_revm;
mod mock;
use criterion::{criterion_group, criterion_main, Criterion};
use mock::{
    init_account, mock_executor_context, mock_transactions, new_rocks_trie_db, new_storage,
};
use std::sync::Arc;

use bench_revm::{revm_exec, RevmAdapter};
use core_executor::{AxonExecutor, AxonExecutorAdapter, MPTTrie};
use protocol::{
    codec::ProtocolCodec,
    traits::{Executor, Storage},
    types::{Account, Address, ExecutorContext},
};

trait BackendInit<S: Storage + 'static, DB: cita_trie::DB + 'static> {
    fn init(
        storage: S,
        db: DB,
        exec_ctx: ExecutorContext,
        init_account: Account,
        addr: Address,
    ) -> Self;
}

impl<S, DB> BackendInit<S, DB> for RevmAdapter<S, DB>
where
    S: Storage + 'static,
    DB: cita_trie::DB + 'static,
{
    fn init(
        storage: S,
        db: DB,
        exec_ctx: ExecutorContext,
        init_account: Account,
        addr: Address,
    ) -> Self {
        let mut revm_adapter = RevmAdapter::new(storage, db, exec_ctx);
        revm_adapter.init_mpt(init_account, addr);
        revm_adapter
    }
}

impl<S, DB> BackendInit<S, DB> for AxonExecutorAdapter<S, DB>
where
    S: Storage + 'static,
    DB: cita_trie::DB + 'static,
{
    fn init(
        storage: S,
        db: DB,
        exec_ctx: ExecutorContext,
        init_account: Account,
        addr: Address,
    ) -> Self {
        let db = Arc::new(db);
        let mut mpt = MPTTrie::new(Arc::clone(&db));

        mpt.insert(addr.as_slice(), init_account.encode().unwrap().as_ref())
            .unwrap();

        let state_root = mpt.commit().unwrap();
        let adapter =
            AxonExecutorAdapter::from_root(state_root, db, Arc::new(storage), exec_ctx).unwrap();
        adapter
    }
}

fn criterion_100_txs(c: &mut Criterion) {
    let txs = mock_transactions(100);
    c.bench_function("revm 100 tx", |b| {
        b.iter(|| {
            let storage = new_storage();
            let db = new_rocks_trie_db();
            let exec_ctx = mock_executor_context();
            let (account, addr) = init_account();
            let revm_adapter = RevmAdapter::init(storage, db, exec_ctx, account, addr);
            revm_exec(revm_adapter, txs.clone());
        });
    });
    c.bench_function("evm 100 tx", |b| {
        b.iter(|| {
            let storage = new_storage();
            let db = new_rocks_trie_db();
            let exec_ctx = mock_executor_context();
            let (account, addr) = init_account();
            let mut axon_adapter = AxonExecutorAdapter::init(storage, db, exec_ctx, account, addr);
            let executor = AxonExecutor::default();
            executor.exec(&mut axon_adapter, txs.clone());
        })
    });
}

criterion_group!(benches, criterion_100_txs,);
criterion_main!(benches);
