FROM confluentinc/cp-kafka:7.6.1

USER root
RUN microdnf install -y iproute && microdnf clean all

COPY configure_routing.sh /configure_routing.sh
RUN chmod +x /configure_routing.sh

ENTRYPOINT ["/configure_routing.sh"]

# We remain running as root; the entrypoint will later drop privileges
# after configuring the routing rules.