mod adapter;
mod client;
mod error;
mod grpc;
mod transfer;

use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use ibc::clients::ics07_tendermint::consensus_state::ConsensusState;
use ibc::timestamp::Timestamp;
use ibc::{
    core::{
        ics02_client::client_consensus::AnyConsensusState,
        ics02_client::context::ClientReader,
        ics02_client::error::Error as ClientError,
        ics02_client::{
            client_state::AnyClientState, client_type::ClientType, context::ClientKeeper,
        },
        ics03_connection::connection::ConnectionEnd,
        ics03_connection::context::{ConnectionKeeper, ConnectionReader},
        ics03_connection::error::Error as ConnectionError,
        ics04_channel::commitment::{AcknowledgementCommitment, PacketCommitment},
        ics04_channel::context::ChannelReader,
        ics04_channel::error::Error as ChannelError,
        ics04_channel::packet::{Receipt, Sequence},
        ics04_channel::{channel::ChannelEnd, context::ChannelKeeper},
        ics05_port::context::PortReader,
        ics05_port::error::Error as PortError,
        ics23_commitment::commitment::CommitmentPrefix,
        ics24_host::identifier::{ChannelId, ClientId, ConnectionId, PortId},
        ics26_routing::context::{Ics26Context, Module, ModuleId, Router},
    },
    Height,
};

use protocol::traits::{IbcAdapter, IbcContext};
use protocol::types::Hasher;

use crate::grpc::GrpcService;

pub async fn run_ibc_grpc<
    Adapter: IbcAdapter + 'static,
    Ctx: Ics26Context + Sync + Send + 'static,
>(
    adapter: Adapter,
    addr: String,
    ctx: Ctx,
) {
    log::info!("ibc start");
    let grpc_service = GrpcService::new(Arc::new(adapter), addr, Arc::new(RwLock::new(ctx)));
    grpc_service.run().await;
}

pub struct IbcImpl<Adapter: IbcContext, Router> {
    adapter:                  Arc<RwLock<Adapter>>,
    router:                   Router,
    client_counter:           u64,
    channel_counter:          u64,
    conn_counter:             u64,
    port_to_module_map:       BTreeMap<PortId, ModuleId>,
    client_processed_times:   HashMap<(ClientId, Height), Timestamp>,
    client_processed_heights: HashMap<(ClientId, Height), Height>,
    consensus_states:         HashMap<u64, ConsensusState>,
}

impl<Adapter: IbcContext, Router> ClientReader for IbcImpl<Adapter, Router> {
    fn client_type(&self, client_id: &ClientId) -> Result<ClientType, ClientError> {
        let adapter = self.adapter.read().unwrap();
        match adapter.get_client_type(client_id) {
            Ok(Some(v)) => Ok(v),
            _ => Err(ClientError::implementation_specific()),
        }
    }

    fn client_state(&self, client_id: &ClientId) -> Result<AnyClientState, ClientError> {
        let adapter = self.adapter.read().unwrap();
        match adapter.get_client_state(client_id) {
            Ok(Some(v)) => Ok(v),
            _ => Err(ClientError::implementation_specific()),
        }
    }

    fn consensus_state(
        &self,
        client_id: &ClientId,
        height: ibc::Height,
    ) -> Result<AnyConsensusState, ClientError> {
        let epoch = height.revision_number();
        let height = height.revision_height();
        let adapter = self.adapter.read().unwrap();
        match adapter.get_consensus_state(client_id, epoch, height) {
            Ok(Some(v)) => Ok(v),
            _ => Err(ClientError::implementation_specific()),
        }
    }

    fn next_consensus_state(
        &self,
        client_id: &ClientId,
        height: ibc::Height,
    ) -> Result<Option<AnyConsensusState>, ClientError> {
        let adapter = self.adapter.read().unwrap();
        match adapter.get_next_consensus_state(client_id, height) {
            Ok(Some(v)) => Ok(Some(v)),
            Ok(None) => Ok(None),
            Err(_) => Err(ClientError::implementation_specific()),
        }
    }

    fn prev_consensus_state(
        &self,
        client_id: &ClientId,
        height: ibc::Height,
    ) -> Result<Option<AnyConsensusState>, ClientError> {
        let adapter = self.adapter.read().unwrap();
        match adapter.get_prev_consensus_state(client_id, height) {
            Ok(Some(v)) => Ok(Some(v)),
            Ok(None) => Ok(None),
            Err(_) => Err(ClientError::implementation_specific()),
        }
    }

    fn host_height(&self) -> ibc::Height {
        let adapter = self.adapter.read().unwrap();
        Height::new(0, adapter.get_current_height()).unwrap()
    }

    fn host_consensus_state(&self, height: ibc::Height) -> Result<AnyConsensusState, ClientError> {
        let consensus_state = self
            .consensus_states
            .get(&height.revision_height())
            .ok_or_else(|| ClientError::missing_local_consensus_state(height))?;
        Ok(AnyConsensusState::Tendermint(consensus_state.clone()))
    }

    fn pending_host_consensus_state(&self) -> Result<AnyConsensusState, ClientError> {
        let pending_height = ClientReader::host_height(self).increment();
        ClientReader::host_consensus_state(self, pending_height)
    }

    fn client_counter(&self) -> Result<u64, ClientError> {
        Ok(self.client_counter)
    }
}

impl<Adapter: IbcContext, Router> ClientKeeper for IbcImpl<Adapter, Router> {
    fn store_client_type(
        &mut self,
        client_id: ClientId,
        client_type: ClientType,
    ) -> Result<(), ClientError> {
        let mut adapter = self.adapter.write().unwrap();
        match adapter.set_client_type(client_id, client_type) {
            Ok(_) => Ok(()),
            Err(_) => Err(ClientError::implementation_specific()),
        }
    }

    fn store_client_state(
        &mut self,
        client_id: ClientId,
        client_state: AnyClientState,
    ) -> Result<(), ClientError> {
        let mut adapter = self.adapter.write().unwrap();
        match adapter.set_client_state(client_id, client_state) {
            Ok(_) => Ok(()),
            Err(_) => Err(ClientError::implementation_specific()),
        }
    }

    fn store_consensus_state(
        &mut self,
        client_id: ClientId,
        height: ibc::Height,
        consensus_state: AnyConsensusState,
    ) -> Result<(), ClientError> {
        let mut adapter = self.adapter.write().unwrap();
        match adapter.set_consensus_state(client_id, height, consensus_state) {
            Ok(_) => Ok(()),
            Err(_) => Err(ClientError::implementation_specific()),
        }
    }

    fn increase_client_counter(&mut self) {
        self.client_counter += 1;
    }

    fn store_update_time(
        &mut self,
        client_id: ClientId,
        height: ibc::Height,
        timestamp: Timestamp,
    ) -> Result<(), ClientError> {
        let _ = self
            .client_processed_times
            .insert((client_id, height), timestamp);
        Ok(())
    }

    fn store_update_height(
        &mut self,
        client_id: ClientId,
        height: ibc::Height,
        host_height: ibc::Height,
    ) -> Result<(), ClientError> {
        let _ = self
            .client_processed_heights
            .insert((client_id, height), host_height);
        Ok(())
    }
}

impl<Adapter: IbcContext, Router> ConnectionKeeper for IbcImpl<Adapter, Router> {
    fn store_connection(
        &mut self,
        connection_id: ConnectionId,
        connection_end: &ConnectionEnd,
    ) -> Result<(), ConnectionError> {
        let mut adapter = self.adapter.write().unwrap();
        match adapter.set_connection_end(connection_id, connection_end.clone()) {
            Ok(_) => Ok(()),
            Err(_) => Err(ConnectionError::implementation_specific()),
        }
    }

    fn store_connection_to_client(
        &mut self,
        connection_id: ConnectionId,
        client_id: &ClientId,
    ) -> Result<(), ConnectionError> {
        let mut adapter = self.adapter.write().unwrap();
        match adapter.set_connection_to_client(connection_id, client_id) {
            Ok(_) => Ok(()),
            Err(_) => Err(ConnectionError::implementation_specific()),
        }
    }

    fn increase_connection_counter(&mut self) {
        self.conn_counter += 1;
    }
}

impl<Adapter: IbcContext, Router> ConnectionReader for IbcImpl<Adapter, Router> {
    fn connection_end(&self, conn_id: &ConnectionId) -> Result<ConnectionEnd, ConnectionError> {
        let adapter = self.adapter.read().unwrap();
        match adapter.get_connection_end(conn_id) {
            Ok(Some(v)) => Ok(v),
            _ => Err(ConnectionError::implementation_specific()),
        }
    }

    fn client_state(&self, client_id: &ClientId) -> Result<AnyClientState, ConnectionError> {
        ClientReader::client_state(self, client_id).map_err(ConnectionError::ics02_client)
    }

    fn host_current_height(&self) -> ibc::Height {
        ClientReader::host_height(self)
    }

    fn host_oldest_height(&self) -> ibc::Height {
        Height::new(0, 1).unwrap()
    }

    fn commitment_prefix(&self) -> CommitmentPrefix {
        CommitmentPrefix::try_from("ibc".as_bytes().to_vec()).unwrap()
    }

    fn client_consensus_state(
        &self,
        client_id: &ClientId,
        height: ibc::Height,
    ) -> Result<AnyConsensusState, ConnectionError> {
        ClientReader::consensus_state(self, client_id, height)
            .map_err(ConnectionError::ics02_client)
    }

    fn host_consensus_state(
        &self,
        height: ibc::Height,
    ) -> Result<AnyConsensusState, ConnectionError> {
        ClientReader::host_consensus_state(self, height).map_err(ConnectionError::ics02_client)
    }

    fn connection_counter(&self) -> Result<u64, ConnectionError> {
        Ok(self.conn_counter)
    }
}

impl<Adapter: IbcContext, Router> PortReader for IbcImpl<Adapter, Router> {
    fn lookup_module_by_port(&self, port_id: &PortId) -> Result<ModuleId, PortError> {
        self.port_to_module_map
            .get(port_id)
            .ok_or_else(|| PortError::unknown_port(port_id.clone()))
            .map(Clone::clone)
    }
}

impl<Adapter: IbcContext, Router> ChannelKeeper for IbcImpl<Adapter, Router> {
    fn store_packet_commitment(
        &mut self,
        key: (PortId, ChannelId, Sequence),
        commitment: PacketCommitment,
    ) -> Result<(), ChannelError> {
        let mut adapter = self.adapter.write().unwrap();
        match adapter.set_packet_commitment(key, commitment) {
            Ok(_) => Ok(()),
            Err(_) => Err(ChannelError::implementation_specific()),
        }
    }

    fn delete_packet_commitment(
        &mut self,
        key: (PortId, ChannelId, Sequence),
    ) -> Result<(), ChannelError> {
        let mut adapter = self.adapter.write().unwrap();
        match adapter.delete_packet_commitment(key) {
            Ok(_) => Ok(()),
            Err(_) => Err(ChannelError::implementation_specific()),
        }
    }

    fn store_packet_receipt(
        &mut self,
        key: (PortId, ChannelId, Sequence),
        receipt: Receipt,
    ) -> Result<(), ChannelError> {
        let mut adapter = self.adapter.write().unwrap();
        match adapter.set_packet_receipt(key, receipt) {
            Ok(_) => Ok(()),
            Err(_) => Err(ChannelError::implementation_specific()),
        }
    }

    fn store_packet_acknowledgement(
        &mut self,
        key: (PortId, ChannelId, Sequence),
        ack_commitment: AcknowledgementCommitment,
    ) -> Result<(), ChannelError> {
        let mut adapter = self.adapter.write().unwrap();
        match adapter.set_packet_acknowledgement(key, ack_commitment) {
            Ok(_) => Ok(()),
            Err(_) => Err(ChannelError::implementation_specific()),
        }
    }

    fn delete_packet_acknowledgement(
        &mut self,
        key: (PortId, ChannelId, Sequence),
    ) -> Result<(), ChannelError> {
        self.store_packet_acknowledgement(key, vec![].into())
    }

    fn store_connection_channels(
        &mut self,
        _conn_id: ConnectionId,
        _port_channel_id: &(PortId, ChannelId),
    ) -> Result<(), ChannelError> {
        todo!()
    }

    fn store_channel(
        &mut self,
        (port_id, chan_id): (PortId, ChannelId),
        channel_end: &ibc::core::ics04_channel::channel::ChannelEnd,
    ) -> Result<(), ChannelError> {
        let mut adapter = self.adapter.write().unwrap();
        match adapter.set_channel(port_id, chan_id, channel_end.clone()) {
            Ok(_) => Ok(()),
            Err(_) => Err(ChannelError::implementation_specific()),
        }
    }

    fn store_next_sequence_send(
        &mut self,
        (port_id, chan_id): (PortId, ChannelId),
        seq: Sequence,
    ) -> Result<(), ChannelError> {
        let mut adapter = self.adapter.write().unwrap();
        match adapter.set_next_sequence_send(port_id, chan_id, seq) {
            Ok(_) => Ok(()),
            Err(_) => Err(ChannelError::implementation_specific()),
        }
    }

    fn store_next_sequence_recv(
        &mut self,
        (port_id, chan_id): (PortId, ChannelId),
        seq: Sequence,
    ) -> Result<(), ChannelError> {
        let mut adapter = self.adapter.write().unwrap();
        match adapter.set_next_sequence_recv(port_id, chan_id, seq) {
            Ok(_) => Ok(()),
            Err(_) => Err(ChannelError::implementation_specific()),
        }
    }

    fn store_next_sequence_ack(
        &mut self,
        (port_id, chan_id): (PortId, ChannelId),
        seq: Sequence,
    ) -> Result<(), ChannelError> {
        let mut adapter = self.adapter.write().unwrap();
        match adapter.set_next_sequence_ack(port_id, chan_id, seq) {
            Ok(_) => Ok(()),
            Err(_) => Err(ChannelError::implementation_specific()),
        }
    }

    fn increase_channel_counter(&mut self) {
        self.channel_counter += 1;
    }
}

impl<Adapter: IbcContext, Router> ChannelReader for IbcImpl<Adapter, Router> {
    fn channel_end(
        &self,
        port_channel_id: &(PortId, ChannelId),
    ) -> Result<ChannelEnd, ChannelError> {
        let adapter = self.adapter.read().unwrap();
        match adapter.get_channel_end(port_channel_id) {
            Ok(Some(v)) => Ok(v),
            _ => Err(ChannelError::implementation_specific()),
        }
    }

    fn connection_end(&self, conn_id: &ConnectionId) -> Result<ConnectionEnd, ChannelError> {
        let adapter = self.adapter.read().unwrap();
        match adapter.get_connection_end(conn_id) {
            Ok(Some(v)) => Ok(v),
            _ => Err(ChannelError::implementation_specific()),
        }
    }

    fn connection_channels(
        &self,
        _cid: &ConnectionId,
    ) -> Result<Vec<(PortId, ChannelId)>, ChannelError> {
        unimplemented!()
    }

    fn client_state(&self, client_id: &ClientId) -> Result<AnyClientState, ChannelError> {
        let adapter = self.adapter.read().unwrap();
        match adapter.get_client_state(client_id) {
            Ok(Some(v)) => Ok(v),
            _ => Err(ChannelError::implementation_specific()),
        }
    }

    fn client_consensus_state(
        &self,
        client_id: &ClientId,
        height: ibc::Height,
    ) -> Result<AnyConsensusState, ChannelError> {
        let epoch = height.revision_number();
        let h = height.revision_height();
        let adapter = self.adapter.read().unwrap();
        match adapter.get_consensus_state(client_id, epoch, h) {
            Ok(Some(v)) => Ok(v),
            _ => Err(ChannelError::implementation_specific()),
        }
    }

    fn get_next_sequence_send(
        &self,
        port_channel_id: &(PortId, ChannelId),
    ) -> Result<Sequence, ChannelError> {
        let adapter = self.adapter.read().unwrap();
        match adapter.get_next_sequence_send(port_channel_id) {
            Ok(Some(v)) => Ok(v),
            _ => Err(ChannelError::implementation_specific()),
        }
    }

    fn get_next_sequence_recv(
        &self,
        _port_channel_id: &(PortId, ChannelId),
    ) -> Result<Sequence, ChannelError> {
        unimplemented!()
    }

    fn get_next_sequence_ack(
        &self,
        _port_channel_id: &(PortId, ChannelId),
    ) -> Result<Sequence, ChannelError> {
        unimplemented!()
    }

    fn get_packet_commitment(
        &self,
        _key: &(PortId, ChannelId, Sequence),
    ) -> Result<PacketCommitment, ChannelError> {
        unimplemented!()
    }

    fn get_packet_receipt(
        &self,
        _key: &(PortId, ChannelId, Sequence),
    ) -> Result<Receipt, ChannelError> {
        unimplemented!()
    }

    fn get_packet_acknowledgement(
        &self,
        _key: &(PortId, ChannelId, Sequence),
    ) -> Result<AcknowledgementCommitment, ChannelError> {
        unimplemented!()
    }

    fn hash(&self, value: Vec<u8>) -> Vec<u8> {
        Hasher::digest(value).as_bytes().to_vec()
    }

    fn host_height(&self) -> ibc::Height {
        let adapter = self.adapter.read().unwrap();
        Height::new(0, adapter.get_current_height()).unwrap()
    }

    fn host_consensus_state(
        &self,
        _height: ibc::Height,
    ) -> Result<AnyConsensusState, ChannelError> {
        unimplemented!()
    }

    fn pending_host_consensus_state(&self) -> Result<AnyConsensusState, ChannelError> {
        unimplemented!()
    }

    fn client_update_time(
        &self,
        _client_id: &ClientId,
        _height: ibc::Height,
    ) -> Result<Timestamp, ChannelError> {
        unimplemented!()
    }

    fn client_update_height(
        &self,
        _client_id: &ClientId,
        _height: ibc::Height,
    ) -> Result<ibc::Height, ChannelError> {
        unimplemented!()
    }

    fn channel_counter(&self) -> Result<u64, ChannelError> {
        unimplemented!()
    }

    fn max_expected_time_per_block(&self) -> std::time::Duration {
        unimplemented!()
    }
}

impl<Adapter: IbcContext> Ics26Context for IbcImpl<Adapter, IbcRouter> {
    type Router = IbcRouter;

    fn router(&self) -> &Self::Router {
        &self.router
    }

    fn router_mut(&mut self) -> &mut Self::Router {
        &mut self.router
    }
}

pub struct IbcRouter {}

impl Router for IbcRouter {
    fn get_route_mut(&mut self, _module_id: &impl Borrow<ModuleId>) -> Option<&mut dyn Module> {
        todo!()
    }

    fn has_route(&self, _module_id: &impl Borrow<ModuleId>) -> bool {
        todo!()
    }
}
