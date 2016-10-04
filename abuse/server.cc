#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <netinet/in.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdarg.h>
#include <assert.h>

#include <cstdlib>
#include <cinttypes>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>

#include "abuse.h"


struct client_info_t {
    struct sockaddr_in   addr_sin; // just for our reference
    int                  mcw_rank; // just for our reference

    ip_addr_t            addr_ip;  // used to address msg_t's to client
    fi_addr_t            addr_fi;  // used to fi_send/fi_write to client
    uint64_t             rdma_key; // used to fi_write to client
};

typedef map<struct sockaddr_in, client_info_t> client_map_t;

// Mapping of client sockaddr_in addresses to meta data about the client
static client_map_t server_map_of_clients;


// Provide operator<() for sockaddr_in so that we can use it as the
// key in an stl::map.
static bool operator<(const sockaddr_in a, const sockaddr_in b)
{
    if (a.sin_family < b.sin_family) {
        return true;
    } else if (a.sin_family > b.sin_family) {
        return false;
    } else if (a.sin_addr.s_addr < b.sin_addr.s_addr) {
        return true;
    } else if (a.sin_addr.s_addr > b.sin_addr.s_addr) {
        return false;
    } else if (a.sin_port > b.sin_port) {
        return true;
    } else {
        return false;
    }
}

static void server_handle_rdma_completion(struct fi_cq_msg_entry &cqe)
{
    // We completed an RDMA write.  Yay!
    logme("Completed an RDMA write\n");

    // Get the CQEC
    assert(cqe.op_context);
    cqe_context_t *cqec;
    cqec = (cqe_context_t*) cqe.op_context;

    // Verify that this cqec is valid
    assert(cqec);
    assert(CQEC_WRITE == cqec->type);
    assert(cqec->buffer);
    assert(cqec->mr);

    // De-register the RDMA memory
    fi_close(&(cqec->mr->fid));

    // Delete the RDMA buffer and the CQEC
    delete[] cqec->buffer;
    delete cqec;
}

static void server_handle_send_completion(struct fi_cq_msg_entry &cqe)
{
    // We completed a send.  Yay!
    logme("Completed a send\n");

    // Get the CQEC
    assert(cqe.op_context);
    cqe_context_t *cqec;
    cqec = (cqe_context_t*) cqe.op_context;

    // Verify that this cqec is valid
    assert(cqec);
    assert(CQEC_SEND == cqec->type);
    assert(cqec->buffer);

    // Delete the send buffer and the CQEC
    delete (msg_t*) cqec->buffer;
    delete cqec;
}

static void server_handle_connect(struct endpoint_t &ep, msg_t *msg)
{
    log_inbound_msg(msg, "MSG_CONNECT");

    // Make sure we don't already know this client
    struct sockaddr_in addr_sin;
    memset(&addr_sin, 0, sizeof(addr_sin));
    addr_sin.sin_family = AF_INET;
    addr_sin.sin_addr.s_addr = msg->from_ip.ip_addr;
    addr_sin.sin_port = msg->from_ip.ip_port_be;

    client_map_t::iterator it;
    it = server_map_of_clients.find(addr_sin);
    if (server_map_of_clients.end() != it) {
        error("...but we're already connected\n");
    }

    // Resolve the client address (so we can send to it)
    int ret;
    fi_addr_t addr_fi;
    ret = fi_av_insert(fidev.av, &addr_sin, 1, &addr_fi, 0, NULL);
    assert(1 == ret);
    logme("fi_av_insert of %s:%d --> fi_addr_t 0x%" PRIx64 "\n",
          inet_ntoa(addr_sin.sin_addr), htons(addr_sin.sin_port), addr_fi);

    // Save the client info in the client map
    struct client_info_t cinfo;
    cinfo.addr_sin = addr_sin;
    cinfo.addr_fi = addr_fi;
    cinfo.addr_ip = msg->from_ip;
    cinfo.rdma_key = msg->u.connect.client_rdma_key;
    cinfo.mcw_rank = msg->from_mcw_rank;
    server_map_of_clients[addr_sin] = cinfo;

    // Send back a CONNECT_ACK
    // Fill header of message
    msg_t *to_client = new msg_t;
    assert(to_client);
    msg_fill_header(ep, to_client, MSG_CONNECT_ACK);
    to_client->to_ip = cinfo.addr_ip;

    // Fill payload of message
    // JMS For the moment, not initiating RDMA client --> server.
    // Just send back a bogus key.
    to_client->u.connect_ack.server_rdma_key = 0;

    // Send the CONNECT_ACK (no need to wait for it here)
    logme("Sending CONNECT_ACK back to %s:%d, fi_addr 0x%" PRIx64 "\n",
          inet_ntoa(cinfo.addr_sin.sin_addr),
          ntohs(cinfo.addr_sin.sin_port),
          cinfo.addr_fi);
    msg_send(ep, to_client, addr_fi);
    logme("Sent CONNECT_ACK back to %s:%d, fi_addr 0x%" PRIx64 "\n",
          inet_ntoa(cinfo.addr_sin.sin_addr),
          ntohs(cinfo.addr_sin.sin_port),
          cinfo.addr_fi);
}

static void server_handle_disconnect(struct endpoint_t &ep, msg_t *msg)
{
    log_inbound_msg(msg, "MSG_DISCONNECT");

    // Make sure we already know this client
    struct sockaddr_in addr_sin;
    memset(&addr_sin, 0, sizeof(addr_sin));
    addr_sin.sin_family = AF_INET;
    addr_sin.sin_addr.s_addr = msg->from_ip.ip_addr;
    addr_sin.sin_port = msg->from_ip.ip_port_be;

    client_map_t::iterator it;
    it = server_map_of_clients.find(addr_sin);
    if (server_map_of_clients.end() == it) {
        error("...but we don't know who this client is!\n");
    }

    // Remove this client from the fi_av
    int ret;
    ret = fi_av_remove(fidev.av, &(it->second.addr_fi), 1, 0);
    assert(0 == ret);
    logme("fi_av_remove of %s:%d --> fi_addr_t 0x%" PRIx64 "\n",
          inet_ntoa(it->second.addr_sin.sin_addr),
          htons(it->second.addr_sin.sin_port),
          it->second.addr_fi);

    // Remove this client from the server_map_of_clients
    server_map_of_clients.erase(it);
}

static void server_handle_solicit_rdma(struct endpoint_t &ep, msg_t *msg)
{
    log_inbound_msg(msg, "SOLICIT_RDMA");

    // Make sure we already know this client
    struct sockaddr_in addr_sin;
    memset(&addr_sin, 0, sizeof(addr_sin));
    addr_sin.sin_family = AF_INET;
    addr_sin.sin_addr.s_addr = msg->from_ip.ip_addr;
    addr_sin.sin_port = msg->from_ip.ip_port_be;

    client_map_t::iterator it;
    it = server_map_of_clients.find(addr_sin);
    if (server_map_of_clients.end() == it) {
        error("...but we don't know who this client is!\n");
    }

    // Randomize the amount of data we're going to RDMA back.  Use the
    // client-requested size as the upper bound.
    size_t len;
    len = msg->u.solicit_rdma.client_rdma_target_len;
    len *= drand48();
    if (0 == len) {
        len = msg->u.solicit_rdma.client_rdma_target_len;
    }

    // Make a buffer to RDMA write, and fill it with a predictable
    // pattern
    uint8_t *rdma_buffer = new uint8_t[len];
    assert(rdma_buffer);
    buffer_pattern_fill(rdma_buffer, len);

    // Make a context to store info about the RDMA buffer (so we can
    // free it when the RDMA write is complete)
    cqe_context_t *cqec = new cqe_context_t;
    assert(cqec);
    memset(cqec, 0, sizeof(*cqec));
    cqec->type = CQEC_WRITE;
    cqec->seq = cqe_seq++;
    cqec->buffer = rdma_buffer;

    int ret;
    ret = fi_mr_reg(fidev.domain, rdma_buffer, len,
                    mr_flags, 0, (uintptr_t) rdma_buffer,
                    0, &(cqec->mr), NULL);
    if (ret != 0) {
        logme("ERROR: fi_mr_reg ret=%d, %s\n", fi_strerror(-ret));
        error("fi_mr_reg(rdma) failed");
    }

    // Do the RDMA write
    logme("RDMA writing to %s:%d (fi_addr 0x%" PRIx64 ", MCW rank %d), dest buffer=%p - %p (len=%" PRIu64 "), seq=%" PRIu64 ", dest key=%" PRIx64 "\n",
          inet_ntoa(it->second.addr_sin.sin_addr),
          ntohs(it->second.addr_sin.sin_port),
          it->second.addr_fi,
          it->second.mcw_rank,
          msg->u.solicit_rdma.client_rdma_target_addr,
          msg->u.solicit_rdma.client_rdma_target_addr + len,
          len,
          cqec->seq,
          it->second.rdma_key);

    ret = fi_write(ep.ep, rdma_buffer, len, fi_mr_desc(cqec->mr),
                   it->second.addr_fi,
                   msg->u.solicit_rdma.client_rdma_target_addr,
                   it->second.rdma_key,
                   cqec);
    assert(0 == ret);

    // Send back a RDMA_SENT
    // Fill header of message
    msg_t *to_client = new msg_t;
    assert(to_client);
    msg_fill_header(ep, to_client, MSG_RDMA_SENT);
    to_client->to_ip = it->second.addr_ip;

    // Fill payload of message
    to_client->u.rdma_sent.client_rdma_target_addr =
        msg->u.solicit_rdma.client_rdma_target_addr;
    to_client->u.rdma_sent.client_rdma_actual_len = len;

    // Send it (no need to wait for it here)
    msg_send(ep, to_client, it->second.addr_fi);
    logme("Sent RDMA_SENT back to %s:%d, fi_addr 0x%" PRIx64 "\n",
          inet_ntoa(it->second.addr_sin.sin_addr),
          ntohs(it->second.addr_sin.sin_port),
          it->second.addr_fi);
}

static void server_handle_receive(struct endpoint_t &ep, struct fi_cq_msg_entry &cqe)
{
    cqe_context_t *cqec;
    cqec = (cqe_context_t*) cqe.op_context;
    assert(cqec->type == CQEC_RECV);

    msg_t *msg = (msg_t*) cqec->buffer;
    msg_verify(ep, msg);

    switch(msg->type) {
    case MSG_CONNECT:
        server_handle_connect(ep, msg);
        break;

    case MSG_DISCONNECT:
        server_handle_disconnect(ep, msg);
        break;

    case MSG_SOLICIT_RDMA:
        server_handle_solicit_rdma(ep, msg);
        break;

    default:
        logme("Got unexpected message type: %d\n", msg->type);
        error("cannot continue");
    }

    // Repost the receive buffer
    post_receive(ep, cqec->buffer, (void*) cqec);
}

//
// Main Server function
//
void server_main()
{
    setup_ofi_device();
    endpoint_t ep;
    setup_ofi_endpoint(ep);

    // Post receives
    post_receives(ep);

    // Broadcast my OFI endpoint IP address+port to all clients
    modex(ep.my_sin);

    uint64_t rma_completion = FI_RMA | FI_MSG | FI_SEND;
    uint64_t send_completion = FI_SEND | FI_MSG;
    uint64_t recv_completion = FI_RECV | FI_MSG;

    // Main server loop
    struct fi_cq_msg_entry cqe;
    while (1) {
        wait_for_cq(ep.cq, cqe);
        if (rma_completion == (cqe.flags & rma_completion)) {
            server_handle_rdma_completion(cqe);
        } else if (send_completion == (cqe.flags & send_completion)) {
            server_handle_send_completion(cqe);
        } else if (recv_completion == (cqe.flags & recv_completion)) {
            // We completed a recive.  Yay!
            server_handle_receive(ep, cqe);
        }
    }

    teardown_ofi_endpoint(ep);
    teardown_ofi_device();
}
