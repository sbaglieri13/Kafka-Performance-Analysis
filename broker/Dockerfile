FROM confluentinc/cp-kafka:7.6.1

USER root
RUN microdnf install -y iproute && microdnf clean all

COPY configure_routing.sh /configure_routing.sh
RUN chmod +x /configure_routing.sh

ENTRYPOINT ["/configure_routing.sh"]

# lasciamo come root, sarà poi l'entrypint a fare il drop di privilegi dopo aver configurato le route
