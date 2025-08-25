#!/bin/bash

# Enhanced Fraud Detection Pipeline Bootstrap Script
# This script runs on EMR cluster startup to configure the environment

set -e

echo "Starting bootstrap process for Enhanced Fraud Detection Pipeline..."

# Update system packages
echo "Updating system packages..."
sudo yum update -y

# Install additional system dependencies
echo "Installing system dependencies..."
sudo yum install -y \
    git \
    wget \
    unzip \
    jq \
    htop \
    iotop \
    nethogs \
    python3-pip \
    python3-devel \
    gcc \
    gcc-c++ \
    make \
    openssl-devel \
    libffi-devel

# Install Python packages
echo "Installing Python packages..."
sudo pip3 install --upgrade pip
sudo pip3 install \
    boto3 \
    botocore \
    requests \
    pandas \
    numpy \
    scikit-learn \
    matplotlib \
    seaborn \
    plotly \
    dash \
    flask \
    gunicorn \
    prometheus-client \
    structlog \
    python-json-logger

# Configure Spark defaults
echo "Configuring Spark defaults..."
sudo tee -a /etc/spark/conf/spark-defaults.conf << EOF

# Enhanced Fraud Detection Pipeline Configuration
spark.executor.memory 8g
spark.executor.cores 4
spark.executor.memoryOverhead 2g
spark.driver.memory 4g
spark.driver.maxResultSize 2g

# Performance tuning
spark.sql.adaptive.enabled true
spark.sql.adaptive.coalescePartitions.enabled true
spark.sql.adaptive.skewJoin.enabled true
spark.sql.adaptive.localShuffleReader.enabled true

# Serialization
spark.serializer org.apache.spark.serializer.KryoSerializer
spark.kryo.registrationRequired false
spark.kryo.unsafe true

# Shuffle optimization
spark.shuffle.file.buffer 32k
spark.shuffle.spill.compress true
spark.shuffle.io.maxRetries 3

# Network configuration
spark.network.timeout 800s
spark.network.io.maxRetries 3
spark.network.io.retryWait 5s

# Dynamic allocation
spark.dynamicAllocation.enabled true
spark.dynamicAllocation.minExecutors 2
spark.dynamicAllocation.maxExecutors 20
spark.dynamicAllocation.initialExecutors 4
spark.dynamicAllocation.executorIdleTimeout 60s
spark.dynamicAllocation.cachedExecutorIdleTimeout 120s

# Streaming optimization
spark.streaming.backpressure.enabled true
spark.streaming.backpressure.initialRate 1000
spark.streaming.kafka.maxOffsetsPerTrigger 10000

# Delta Lake configuration
spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
EOF

# Configure Hadoop environment
echo "Configuring Hadoop environment..."
sudo tee -a /etc/hadoop/conf/hadoop-env.sh << EOF

# Enhanced Fraud Detection Pipeline Hadoop Configuration
export HADOOP_OPTS="-Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
export HADOOP_HEAPSIZE=4096
export HADOOP_NAMENODE_OPTS="-Xmx2g -XX:+UseG1GC"
export HADOOP_DATANODE_OPTS="-Xmx1g -XX:+UseG1GC"
export HADOOP_CLIENT_OPTS="-Xmx2g -XX:+UseG1GC"

# Performance tuning
export HADOOP_CLIENT_OPTS="\$HADOOP_CLIENT_OPTS -Djava.net.preferIPv4Stack=true"
export HADOOP_OPTS="\$HADOOP_OPTS -Djava.net.preferIPv4Stack=true"
EOF

# Configure YARN
echo "Configuring YARN..."
sudo tee -a /etc/hadoop/conf/yarn-site.xml << EOF
<property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>16384</value>
</property>
<property>
    <name>yarn.scheduler.maximum-allocation-mb</value>
    <value>8192</value>
</property>
<property>
    <name>yarn.scheduler.minimum-allocation-mb</value>
    <value>1024</value>
</property>
<property>
    <name>yarn.nodemanager.vmem-pmem-ratio</name>
    <value>2.1</value>
</property>
EOF

# Create application directories
echo "Creating application directories..."
sudo mkdir -p /opt/fraud-detection/{logs,config,models,data}
sudo chown -R hadoop:hadoop /opt/fraud-detection

# Configure logging
echo "Configuring logging..."
sudo tee /opt/fraud-detection/config/logging.conf << EOF
[loggers]
keys=root,fraud_detection

[handlers]
keys=consoleHandler,fileHandler

[formatters]
keys=simpleFormatter

[logger_root]
level=INFO
handlers=consoleHandler

[logger_fraud_detection]
level=INFO
handlers=consoleHandler,fileHandler
qualname=fraud_detection
propagate=0

[handler_consoleHandler]
class=StreamHandler
level=INFO
formatter=simpleFormatter
args=(sys.stdout,)

[handler_fileHandler]
class=FileHandler
level=INFO
formatter=simpleFormatter
args=('/opt/fraud-detection/logs/fraud_detection.log', 'a')

[formatter_simpleFormatter]
format=%(asctime)s - %(name)s - %(levelname)s - %(message)s
datefmt=%Y-%m-%d %H:%M:%S
EOF

# Setup monitoring scripts
echo "Setting up monitoring scripts..."
sudo tee /opt/fraud-detection/monitor_cluster.sh << 'EOF'
#!/bin/bash

# Cluster monitoring script
echo "=== EMR Cluster Status ==="
echo "Timestamp: $(date)"
echo ""

echo "=== YARN Applications ==="
yarn application -list -appStates ALL | head -20
echo ""

echo "=== Cluster Metrics ==="
yarn node -list -all | head -10
echo ""

echo "=== Resource Usage ==="
yarn node -status $(yarn node -list | grep -o '^[^[:space:]]*' | head -1) 2>/dev/null | grep -E "(Memory|CPU)" || echo "Unable to get node status"
echo ""

echo "=== Disk Usage ==="
df -h | grep -E "(Filesystem|/mnt|/dev)"
echo ""

echo "=== Memory Usage ==="
free -h
echo ""

echo "=== Process Count ==="
echo "Java processes: $(pgrep -c java)"
echo "Python processes: $(pgrep -c python)"
echo ""

echo "=== Network Connections ==="
netstat -an | grep -E ":(9092|3306|443|80)" | wc -l | xargs echo "Active connections to key ports:"
EOF

sudo chmod +x /opt/fraud-detection/monitor_cluster.sh

# Setup health check script
echo "Setting up health check script..."
sudo tee /opt/fraud-detection/health_check.sh << 'EOF'
#!/bin/bash

# Health check script for fraud detection pipeline
HEALTH_STATUS=0

echo "Running health checks..."

# Check YARN status
if yarn application -list -appStates RUNNING | grep -q "fraud"; then
    echo "✓ Fraud detection application is running"
else
    echo "✗ Fraud detection application is not running"
    HEALTH_STATUS=1
fi

# Check Kafka connectivity
if nc -z pkc-p11xm.us-east-1.aws.confluent.cloud 9092 2>/dev/null; then
    echo "✓ Kafka connection is available"
else
    echo "✗ Kafka connection failed"
    HEALTH_STATUS=1
fi

# Check S3 access
if aws s3 ls s3://athenabucket-231/ >/dev/null 2>&1; then
    echo "✓ S3 access is working"
else
    echo "✗ S3 access failed"
    HEALTH_STATUS=1
fi

# Check RDS connectivity
if nc -z grouptwo.cq7tmru9zell.us-east-1.rds.amazonaws.com 3306 2>/dev/null; then
    echo "✓ RDS connection is available"
else
    echo "✗ RDS connection failed"
    HEALTH_STATUS=1
fi

# Check disk space
DISK_USAGE=$(df /mnt | tail -1 | awk '{print $5}' | sed 's/%//')
if [ "$DISK_USAGE" -lt 80 ]; then
    echo "✓ Disk usage is acceptable: ${DISK_USAGE}%"
else
    echo "✗ Disk usage is high: ${DISK_USAGE}%"
    HEALTH_STATUS=1
fi

# Check memory usage
MEM_USAGE=$(free | grep Mem | awk '{printf("%.0f", $3/$2 * 100.0)}')
if [ "$MEM_USAGE" -lt 80 ]; then
    echo "✓ Memory usage is acceptable: ${MEM_USAGE}%"
else
    echo "✗ Memory usage is high: ${MEM_USAGE}%"
    HEALTH_STATUS=1
fi

echo ""
if [ $HEALTH_STATUS -eq 0 ]; then
    echo "✓ All health checks passed"
    exit 0
else
    echo "✗ Some health checks failed"
    exit 1
fi
EOF

sudo chmod +x /opt/fraud-detection/health_check.sh

# Setup cron jobs for monitoring
echo "Setting up monitoring cron jobs..."
sudo tee /etc/cron.d/fraud-detection-monitoring << EOF
# Fraud Detection Pipeline Monitoring
*/5 * * * * hadoop /opt/fraud-detection/monitor_cluster.sh >> /opt/fraud-detection/logs/cluster_monitor.log 2>&1
*/10 * * * * hadoop /opt/fraud-detection/health_check.sh >> /opt/fraud-detection/logs/health_check.log 2>&1
0 * * * * hadoop find /opt/fraud-detection/logs -name "*.log" -mtime +7 -delete
EOF

# Configure log rotation
echo "Configuring log rotation..."
sudo tee /etc/logrotate.d/fraud-detection << EOF
/opt/fraud-detection/logs/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 hadoop hadoop
    postrotate
        /bin/kill -HUP \`cat /var/run/syslogd.pid 2>/dev/null\` 2>/dev/null || true
    endscript
}
EOF

# Setup performance tuning
echo "Setting up performance tuning..."
sudo tee /etc/sysctl.conf << EOF

# Enhanced Fraud Detection Pipeline Performance Tuning
# Network tuning
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
net.core.netdev_max_backlog = 5000
net.ipv4.tcp_congestion_control = bbr

# File system tuning
fs.file-max = 1000000
fs.inotify.max_user_watches = 1000000

# Memory tuning
vm.swappiness = 1
vm.dirty_ratio = 15
vm.dirty_background_ratio = 5
EOF

# Apply sysctl changes
sudo sysctl -p

# Setup firewall rules (if needed)
echo "Setting up firewall rules..."
sudo systemctl start firewalld 2>/dev/null || true
sudo firewall-cmd --permanent --add-port=8080/tcp 2>/dev/null || true
sudo firewall-cmd --permanent --add-port=4040/tcp 2>/dev/null || true
sudo firewall-cmd --reload 2>/dev/null || true

# Create startup script
echo "Creating startup script..."
sudo tee /opt/fraud-detection/startup.sh << 'EOF'
#!/bin/bash

# Startup script for fraud detection pipeline
echo "Starting fraud detection pipeline services..."

# Start monitoring
/opt/fraud-detection/monitor_cluster.sh &
/opt/fraud-detection/health_check.sh &

# Wait for services to be ready
sleep 30

echo "Fraud detection pipeline startup completed"
EOF

sudo chmod +x /opt/fraud-detection/startup.sh

# Setup environment variables
echo "Setting up environment variables..."
sudo tee /etc/profile.d/fraud-detection.sh << EOF

# Enhanced Fraud Detection Pipeline Environment Variables
export FRAUD_DETECTION_HOME=/opt/fraud-detection
export FRAUD_DETECTION_CONFIG=\$FRAUD_DETECTION_HOME/config
export FRAUD_DETECTION_LOGS=\$FRAUD_DETECTION_HOME/logs
export FRAUD_DETECTION_MODELS=\$FRAUD_DETECTION_HOME/models
export FRAUD_DETECTION_DATA=\$FRAUD_DETECTION_HOME/data

# Add to PATH
export PATH=\$PATH:\$FRAUD_DETECTION_HOME

# Python path
export PYTHONPATH=\$PYTHONPATH:\$FRAUD_DETECTION_HOME

# Logging
export PYTHONUNBUFFERED=1
export PYTHONIOENCODING=utf-8
EOF

# Source environment variables
source /etc/profile.d/fraud-detection.sh

# Final setup
echo "Finalizing setup..."
sudo chown -R hadoop:hadoop /opt/fraud-detection
sudo chmod -R 755 /opt/fraud-detection

# Create symbolic links
sudo ln -sf /opt/fraud-detection/monitor_cluster.sh /usr/local/bin/monitor-fraud-detection
sudo ln -sf /opt/fraud-detection/health_check.sh /usr/local/bin/health-check-fraud-detection

echo "Bootstrap process completed successfully!"
echo "Fraud detection pipeline is ready for deployment."

# Run initial health check
echo "Running initial health check..."
/opt/fraud-detection/health_check.sh

echo "Bootstrap script execution completed at $(date)"

