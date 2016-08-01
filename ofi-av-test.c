#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdarg.h>
#include <assert.h>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>


static void test_av()
{



    JMS copy more to open an info, domain




    // Make an AV
    struct fi_av_attr av_attr = {
        .type = FI_AV_UNSPEC,
        .count = 32
    };
    struct fid_av av;
    ret = fi_av_open(fidev.domain, &av_attr, &av, NULL);
    if (0 != ret) {
        error("fi_av_open failed");
    }

    teardown_ofi();
}

////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[])
{
    test_av();

    return 0;
}
