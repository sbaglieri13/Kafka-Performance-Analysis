FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y iproute2 iputils-ping iptables net-tools tcpdump dos2unix && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY configure_routing.sh /configure_routing.sh
COPY network_rules.conf /network_rules.conf

RUN dos2unix /configure_routing.sh && \
    chmod +x /configure_routing.sh

ENTRYPOINT ["/configure_routing.sh"]
CMD ["tail", "-f", "/dev/null"]
