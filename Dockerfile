FROM python:3.9-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY udp_transfer.py .
COPY entrypoint.sh .

# Make entrypoint executable
RUN chmod +x entrypoint.sh

# Create a non-root user to run the application
RUN useradd -m appuser
USER appuser

# Set up volume for data
VOLUME ["/data"]

# Expose UDP port
EXPOSE 8000/udp

# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Default command (can be overridden)
CMD ["--help"]
