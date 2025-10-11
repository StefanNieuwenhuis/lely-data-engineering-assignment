FROM apache/kafka:3.9.1

# Set work dir
WORKDIR /opt/kafka

# Expose the port
EXPOSE 8083

# Create plugin folder
RUN mkdir -p /opt/kafka/plugins

# Copy and run plugin installation script
COPY plugins.sh .
RUN chmod +x plugins.sh && ./plugins.sh

# Copy entrypoint script and make it executable
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Use entrypoint
ENTRYPOINT ["./entrypoint.sh"]

# Default command "start" (see entrypoint.sh)
CMD ["start"]