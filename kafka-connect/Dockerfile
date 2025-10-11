FROM apache/kafka:3.9.1

# Set work dir
WORKDIR /opt/kafka

# Expose the port
EXPOSE 8083

# Create plugin folder
RUN mkdir -p /opt/kafka/plugins

# Copy and run plugin installation script
COPY --chmod=+x ./kafka-connect/plugins.sh .
RUN ./plugins.sh


# Copy entrypoint script and make it executable
COPY --chmod=+x ./kafka-connect/entrypoint.sh .
RUN ./entrypoint.sh

# Use entrypoint
ENTRYPOINT ["./entrypoint.sh"]

# Default command "start" (see entrypoint.sh)
CMD ["start"]