<p align="right">
  <b>English</b> | <a href="README.it.md">Italiano</a>
</p>

# Kafka Performance Analysis

[![Docker](https://img.shields.io/badge/Docker-29.2.1-blue.svg)](https://www.docker.com/)
[![Docker%20Compose](https://img.shields.io/badge/DockerCompose-v5.0.2-blue.svg)](https://docs.docker.com/compose/)
[![Python](https://img.shields.io/badge/Python-3.11.14-green.svg)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Kafka-3.8.0-orange.svg)](https://kafka.apache.org/)


## 📖 Overview

This project implements a distributed Kafka environment for performance analysis under controlled network conditions. It includes:
- **3 Kafka brokers** in KRaft mode
- **2 producers** that generate messages
- **3 consumers** split into 2 groups
- **Network Emulator** to simulate controlled network conditions (delay/jitter with variable distribution, packet loss, packet duplication)

Each of these components has been implemented as an independent Docker service.

## 🏗️ Architecture

![Architecture](./docs/software_architecture.png)

### Main components

#### Kafka brokers (3 instances)

- KRaft mode 
- Ports: 9092, 9093, 9094 (host), 29092 (inter-broker)
- JMX ports: 9999, 10000, 10001 for metrics
- Network: `net_brokers` (172.20.10.0/24)
- Replication Factor: 3 (hardcoded)
- Min ISR: 2 (minimum number of "in-sync" replicas)

#### Producers (2 instances)

- Generate messages in two modes: Poisson / Continuous
- Forward them to Kafka with configurable batching
- Send to a single topic per experiment (not multi-topic)
- Write `data/stats/*_stats.json` files with:
  - metrics useful for the experiments
  - overhead and throughput measured by the brokers via JMX
- Save the recap of the generated and forwarded messages in `data/producer_*_messages_recap.jsonl`
- Network: `net_producers` (172.20.20.0/24)

#### Consumers (3 instances)

- 2 consumers in `group_1`, 1 consumer in `group_2`
- Track reception timestamps for the latency analysis
- Save the recap of the processed messages in `data/consumer_*_messages_recap.jsonl`
- Networks: `net_consumers_g1` (172.20.30.0/24), `net_consumers_g2` (172.20.40.0/24)

#### Network Emulator

- Central router connected to all subnets
- Applies `tc/netem` rules to simulate delay with variable distribution, packet duplication and packet loss

#### Kafka UI

- Web interface to monitor the cluster on port 8080


## 📁 Project Structure

```
.
├── broker/                          
│   ├── Dockerfile
│   └── configure_routing.sh
├── producer/                       
│   ├── Dockerfile
│   ├── producer.py
│   ├── configure_routing.sh
│   └── requirements.txt
├── consumer/                        
│   ├── Dockerfile
│   ├── consumer.py
│   ├── configure_routing.sh
│   └── requirements.txt
├── network_emulator/                
│   ├── Dockerfile
│   ├── configure_routing.sh
│   └── network_rules.conf
├── experiments/
│   ├── latency_analysis/
│   |   ├── plots/                        
│   |   |   |── latency_boxplot_of_latency_vs_nb.png  
│   |   |   |── latency_heatmap_of_latency_vs_nb.png  
│   |   |   |── latency_ratio_vs_partition_number.png  
│   |   |   |── latency_vs_lambda.png 
│   |   |   └── latency_vs_nb.png  
│   |   ├── reports/      
│   │   |── generate_report_latency.py
│   │   |── latency_analysis_launch.sh
│   │   └── plot_latency_analysis.py
│   ├── correlation_analysis/
│   |   ├── plots/
│   |   |   └── correlation_analysis.png      
│   |   ├── correlation_analysis_launch.sh 
│   |   └── plot_correlation_analysis.py  
│   ├── poisson_validation/
│   |   ├── plots/
│   |   |   |── poisson_validation_lambda_rate_10.png    
│   |   |   |── poisson_validation_lambda_rate_50.png
│   |   |   └── poisson_validation_lambda_rate_200.png    
│   |   └── plot_poisson_validation.py 
│   └── network_bandwidth_evaluation/
│       ├── plots/
│       |   └── network_bandwidth_evaluation.png     
│       ├── network_bandwidth_evaluation_launch.sh 
│       └── plot_network_bandwidth_evaluation.py 
├── scripts/
|   |── modules/                      
│   | ├── docker_build.sh         
│   | ├── docker_deploy.sh       
│   | ├── experiment_config.sh        
│   | ├── experiment_timer.sh        
│   | └── network_config.sh      
|   ├── custom_setup_environment.sh  
│   ├── teardown_environment.sh     
│   └── update_stats_with_jmx.py     
├── utilities/
|   ├── __init__.py  
│   ├── confidence_interval.py     
│   └── tukey_iqr.py    
├── data/                            
|   └── stats/ 
├── docs/ 
├── docker-compose.yml               
├── .gitignore
├── README.md                        
└── requirements.txt                 
```

## 🛠️ Utilities

The `utilities/` folder contains Python modules reused by all the experiment scripts for outlier filtering and multi-run statistical estimation.

### `tukey_iqr.py` — Tukey IQR outlier filter
Removes outliers from an array of values using Tukey's method based on
the Interquartile Range (IQR). The exclusion criterion is:

$$\text{lower} = Q_1 - 1.5 \cdot IQR \qquad \text{upper} = Q_3 + 1.5 \cdot IQR$$.

All the values that fall outside the `[lower, upper]` range are considered outliers and discarded. This filter is applied across different runs of the same configuration.

### `confidence_interval.py` — 95% Confidence Interval
Computes the point estimate and the 95% confidence interval on the IQR-filtered runs, using Student's t-distribution:
$$CI = \bar{x} \pm t^*_{n-1} \cdot \frac{s}{\sqrt{n}}$$
where:
- $\bar{x}$ is the sample mean
- $s$ is the sample standard deviation
- $n$ is the number of valid runs after the IQR filter
- $t^*_{n-1}$ is the critical value of the t-distribution with $n-1$ degrees of freedom

The representative value used in the plots is the midpoint of the interval:
$$\mathrm{representative\_point} = \frac{CI_{\mathrm{lower}} + CI_{\mathrm{upper}}}{2}$$

**Student's t-distribution** is used instead of the z-test because the number of available samples is small (5 runs for the latency analysis, 10 for the other
experiments). With small samples, the t-distribution produces more conservative intervals and therefore more accurate estimates of the real uncertainty, compensating for the underestimation of the sample standard deviation typical of small $n$.



## 🚀 Quick Start

### Prerequisites

Before starting, make sure you have installed:

- **Docker** and **Docker Compose**
- **Python** 
- **Git** (to clone the repository)

### Python Environment Setup

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# Linux/Mac:
source venv/bin/activate
# Windows (Git Bash):
source venv/Scripts/activate

# Install dependencies
pip install -r requirements.txt

# Create output directory
mkdir -p data/stats
```

## 🧪 Experiments

### 1. Single Experiment

```bash
./scripts/custom_setup_environment.sh
```

Required parameters:
1. **Experiment mode:**
  - Time-based (fixed duration in seconds) 
  - Message-based (target number of messages)
2. **Network Emulation:**
  - Delay/jitter with variable distribution (uniform | normal | pareto | paretonormal), packet loss and packet duplication for each subnet
3. **Topic:**
  - Number of partitions (3, 6, 9, 12)
  - Payload Size: message size in bytes
  - Batch Size: messages per batch
  - Duration/Target: seconds or number of messages
4. **Message generation mode:**
   - **Poisson** (`SEND_MODE=poisson`): Rate-limited Poisson arrivals
      - `LOW`: λ=10 msg/s  → theoretical inter-arrival ≈ 100ms
      - `MEDIUM`: λ=50 msg/s  → theoretical inter-arrival ≈ 20ms
      - `HIGH`: λ=200 msg/s → theoretical inter-arrival ≈ 5ms
   - **Continuous** (`SEND_MODE=continuous`): Heavy load, no rate limiting


This script:
- Configures the network emulation rules (delay, packet loss and duplication)
- Sets the duration of the experiment or the target number of messages to generate 
- Sets payload size, number of messages per batch, etc.
- Sets the mode in which the producers generate the messages
- Builds the Docker images
- Starts all the services

During the execution:
- **Minutes 0-1:** Service initialization phase
- **Minutes 1+:** Message production and consumption
- **End:** JMX metrics collection and generation of the stats files

At the end of the experiment, the recap of the generated messages will be available in the `data` folder, and various statistics, including throughput, overhead and other useful metrics, in the `data\stats` folder. 

---

### 2. Poisson Process Validation
**Goal:** verify that the `SEND_MODE=poisson` mode actually produces a message stream with inter-arrival times distributed according to a Poisson process. 

This model correctly represents real systems in which messages arrive in a **stochastic and independent** way, unlike the `continuous` mode, which generates an artificially continuous stream at maximum throughput.

```bash
# Generate data (the Poisson mode must be selected)
./scripts/custom_setup_environment.sh

# Generate the validation plots
python ./experiments/poisson_validation/plot_poisson_validation.py
```

**Outlier filtering:** 
Before generating the plots, the inter-arrival times are filtered to remove anomalous events. The filtering criterion is based on the theoretical exponential distribution:
given a significance level `α = 0.01`, the following threshold is computed:
$$x_{thr} = \frac{-\ln(\alpha)}{\lambda}$$
which represents the value beyond which the probability of observing an inter-arrival time is lower than 1% according to the theoretical exponential model. All the inter-arrival times greater than $x_{thr}$ are discarded as outliers.

**Generated plots:** 
The three subplots test **distinct and complementary** properties of the Poisson process.
  1. `Inter-arrival Distribution`: shows the distribution of the times between consecutive messages, overlaid on the theoretical exponential Probability Density Function $$f(t) = \lambda \cdot e^{-\lambda t}$$. If the process is correctly Poisson, the histogram bars must line up with the theoretical curve. This confirms that the messages are generated with exponentially distributed inter-arrival times. The inter-arrival times are computed as consecutive differences of the `send_timestamp_ms` field of the `producer_*_messages_recap.jsonl` files.

  2. `Exponential Q-Q Plot`: the Q-Q plot compares the empirical quantiles of the inter-arrival times with the theoretical quantiles of the exponential distribution. If the points line up on the diagonal y = x (ideal), the data actually follow an exponential distribution. 
  3. `Message Count per Window`: shows the empirical distribution of the number of messages received in given time windows, overlaid on the theoretical Poisson PMF (Probability Mass Function) $$P(k) = \frac{\lambda^k \cdot e^{-\lambda}}{k!}$$
  The window is chosen so that the **theoretical mean number of messages per window is always 10** (`μ = λ · window = λ · 10/λ = 10`), regardless of the value of λ. This makes the plots comparable across the different load configurations (Low, Medium, High), preventing the distribution from shifting to different scales, which would make the plot hard to visualize and understand.
  This choice is statistically valid because, by the scaling property of the Poisson process, the number of arrivals in an interval $T$ follows $\text{Poisson}(\mu = \lambda T)$. Fixing $T = 10/\lambda$ imposes $\mu = 10$ for any $\lambda$, preserving the shape of the distribution and the correctness of the comparison with the theoretical PMF.


- **`λ = 10 Low performance`**: 

  ![Poisson low](./experiments/poisson_validation/plots/poisson_validation_lambda_rate_10.png)

- **`λ = 50 Medium performance`**: 

  ![Poisson medium](./experiments/poisson_validation/plots/poisson_validation_lambda_rate_50.png)

- **`λ = 200 High performance`**: 

  ![Poisson high](./experiments/poisson_validation/plots/poisson_validation_lambda_rate_200.png)

--- 

### 3. Latency Analysis
**Goal:** analyze the end-to-end latency (producer → broker → consumer).
```bash
# 1. Runs 5 runs x 216 experiments (4 partitions x 9 batch sizes x 6 lambda rates) 
./experiments/latency_analysis/latency_analysis_launch.sh

# 2. Generate plots
python ./experiments/latency_analysis/plot_latency_analysis.py
```

> **Note:** `generate_report_latency.py` is run automatically at the end
> of each run by the `latency_analysis_launch.sh` script. It generates the
> `experiments/latency_analysis/reports/latency_report.json` file with the
> aggregated statistics per run, which is then read by `plot_latency_analysis.py` to
> produce the final plots.


**Configuration:**
  - Partition counts: 3, 6, 9, 12
  - Batch sizes: 1, 5, 10, 15, 20, 25, 30, 40, 50
  - Lambda rates: 50, 100, 200, 500, 800, 1000
  - Payload size: 200 bytes
  - Number of runs per configuration: 5
  - Duration: 60s production + 30s warmup
  - Message generation mode: Poisson

**Average duration of the whole experiment:** ~52 hours

**Computed metrics:**
**Latency** = `receive_timestamp_ms - producer_send_timestamp_ms`

The end-to-end latency can be decomposed as:
$$L_{e2e} = T_{buffer} + T_{broker} + T_{consumer}$$

where:
- $T_{buffer}$: waiting time in the producer buffer until the batch is full
- $T_{broker}$: time for writing, replication across brokers and commit on the topic
- $T_{consumer}$: polling and processing time on the consumer side

**Multi-run aggregation:**
For each configuration (λ, Nb, partitions), the 5 runs are aggregated with:
  - Outlier filter: Tukey IQR on the mean latency of each run
  - Estimate: for each metric, the 95% confidence interval (t-distribution) is computed on the filtered runs; the representative value is the midpoint of the interval $\mathrm{representative\_point} = \frac{CI_{\mathrm{lower}} + CI_{\mathrm{upper}}}{2}$

**Generated plots:** 
  1. `latency_vs_nb.png`: Latency (s) vs Nb, curves per λ, with adaptive regression and R²
  2. `latency_vs_lambda.png`: Latency (s) vs λ (log scale), curves per Nb, with **power-hyperbolic** fit
  3. `latency_heatmap_of_latency_vs_nb.png`: λ × Nb heatmap with color shading for the mean latency
  4. `latency_boxplot_of_latency_vs_nb.png`: Latency vs Nb boxplot, partitions × λ grid
  5. `latency_partition_ratio_table.png`: Table of the latency ratio as the number of partitions varies, for each configuration (λ, Nb)

  > **R²** (coefficient of determination) measures the proportion of the variance of the
  > data explained by the regression model. It ranges from 0 to 1: a value close
  > to 1 indicates that the model fits the data well; a low value indicates that
  > the assumed relationship does not adequately capture the observed behaviour.


  - **`Boxplot of Latency vs Nb`**: 

  ![Latency Analysis 1](./experiments/latency_analysis/plots/latency_boxplot_of_latency_vs_nb.png)

  The `latency_boxplot_of_latency_vs_nb.png` boxplots highlight several structural behaviours:
  - **Box width grows with Nb:** as the batch size increases, the latency distribution widens, showing that batching not only increases the mean latency but also its **variability**.
  - **Positive skewness:** the upper whisker is systematically longer than the lower one; some messages experience latencies much higher than the median, especially for high Nb.
  - **Narrower boxes at high λ:** with high rates the boxes are more compact, confirming that a high λ reduces both the mean latency and its dispersion: batches fill up quickly and more uniformly.
  - **Behaviour at low λ and many partitions:** in the last row (λ=10), for high Nb the boxes tend to become similar to each other regardless of Nb and of the number of partitions. In this regime $\frac{\lambda}{P}$ is so low that the theoretical batch fill time exceeds `linger_ms = 30000 ms`. Batches are therefore sent on timeout, often only partially full, and the observed latency is dominated by the value of `linger_ms`. Increasing the batch size no longer produces significant changes in the latency distribution.

  - **`Heatmap of Latency vs Nb`**: 

    ![Latency Analysis 2](./experiments/latency_analysis/plots/latency_heatmap_of_latency_vs_nb.png)

  - **`Ratio vs Partition`**: 

    ![Latency Analysis 3](./experiments/latency_analysis/plots/latency_ratio_vs_partition_number.png)

    The plot shows the **latency ratio** with respect to P=3 for each (λ, Nb). In the normal regime the values systematically approach **×2** (P=6), **×3** (P=9), **×4** (P=12) because:
    With P partitions the producer keeps **P parallel internal buffers** (one per partition). For the same λ, the message stream is distributed evenly across the buffers: each buffer receives only **λ/P msg/s**.
    The time needed to fill a batch of Nb messages becomes:
    $T_{buffer} = \frac{Nb}{\lambda/P} = \frac{Nb \cdot P}{\lambda}$. The latency **scales with P**. 
    
    For λ=10 and **Nb≥30**, the ratios collapse towards ×1.7-1.8 instead of ×4. Here λ is so low that the batches **never fill up within `linger_ms = 30000 ms`**. Sending happens on timeout with partially full batches, making the latency independent of P (dominated by the fixed timeout).

  - **`Latency vs λ`**: 

    ![Latency Analysis 4](./experiments/latency_analysis/plots/latency_vs_lambda.png)

    Latency (s) vs λ (log scale), curves per Nb, with **power-hyperbolic** fit:
    $$L(\lambda) = \frac{a}{\lambda^b} + c$$
     
    This model generalizes the classic hyperbola $\frac{a}{\lambda} + c$ by adding the exponent $b$: for small Nb the data are compatible with $b \approx 1$, while for large Nb the curves become flatter $(b < 1)$ because, beyond a certain λ threshold, the latency is dominated by $T_{broker}$ and $T_{consumer}$ and stops decreasing in a purely hyperbolic way.
    In the plot with **P=12** this effect is particularly visible: the curves for high Nb no longer fit the power-hyperbolic model perfectly and show a slight misalignment between points and prediction. This is not a fitting error but the graphical manifestation of the transition to the timeout-dominated regime induced by `linger_ms`.

  - **`Latency vs Nb`**: 

    ![Latency Analysis 5](./experiments/latency_analysis/plots/latency_vs_nb.png)

    Latency (s) vs Nb, curves per λ, with adaptive regression and R²:
     - **Linear regression** $L = a \cdot N_b + c$ when R² ≥ 0.95, which describes the regime in which the latency grows almost linearly with the batch size because the dominant term is the buffer fill time $T_{buffer}$.
     - **Quadratic regression** $L = a \cdot N_b^2 + b \cdot N_b + c$ (label: `quadratic fit`) when R² < 0.95, typically for very low λ and many topic partitions. In this case the system enters a saturation / queuing regime and the latency increase with Nb becomes super-linear.

#### Comparison between heavy load and Poisson

In the **heavy load** case, the producer generates messages at maximum throughput; the incoming stream is high enough to fill the batches well before `linger_ms = 30000 ms` expires. In this regime the system is **efficient enough** not to hit the limit imposed by `linger_ms`, and the batches are practically always sent at full capacity.

The **Poisson** mode, instead, also makes it possible to explore partial load scenarios: when $\lambda$ is large enough to fill a batch within `linger_ms`, the latency follows the theoretical models based on $T_{buffer}$ (linear fit as a function of Nb, power-hyperbolic as a function of λ). When instead $\frac{\lambda}{P}$ is so low that the batch cannot be filled in time, a regime change is observed: batches are sent early because the timer expires and the latency approaches `linger_ms`, becoming insensitive to variations of Nb and P.

---

### 4. Network Bandwidth Evaluation
**Goal:** evaluate throughput and overhead as batch size and number of partitions vary.
```bash
# Runs 10 runs x 120 experiments (4 partitions × 30 batch sizes)
./experiments/network_bandwidth_evaluation/network_bandwidth_evaluation_launch.sh

# Generate plot
python ./experiments/network_bandwidth_evaluation/plot_network_bandwidth_evaluation.py
```
**Configuration:**
  - Partition counts: 3, 6, 9, 12
  - Batch sizes: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 15, 16, 18, 20, 21, 23, 24, 26, 29, 30, 32, 35, 37, 40, 41, 45, 49, 50
  - Payload size: 1000 bytes
  - Number of runs per configuration: 10
  - Target messages: 50K
  - Message generation mode: Heavy mode

**Average duration of the whole experiment:** ~40 hours

**Computed metrics:**
  1. Kafka Total Bytes (JMX):
  **kafka_total_bytes** = $BytesInPerSec$
  2. Kafka Overhead:
  **kafka_overhead_bytes** = $kafkaOverheadBytes = kafkaTotalBytes - payloadBytes$
  3. Overhead Percentage:
  **overhead_%** = $\frac{kafkaOverheadBytes}{kafkaTotalBytes} \times 100$
  4. Throughput:
  **throughput_kbps** = $\frac{kafkaTotalBytes / 1024}{transmissionTimeSeconds}$

**Multi-run aggregation:**
For each configuration (partitions, batch size), the runs are aggregated with:
  - Outlier filter: Tukey IQR on throughput to exclude anomalous runs
  - Estimate: 95% confidence interval (t-distribution) on the filtered runs; the representative value is $\mathrm{representativePoint} = \frac{CI_{\mathrm{lower}} + CI_{\mathrm{upper}}}{2}$

**Fitting models:**
  - Throughput (exponential saturation):
  $$T(x) = c - a \cdot e^{-bx}$$
  - Overhead (power-law decay):
  $$O(x) = a \cdot x^{-b} + c$$

**Plot:**
The plot shows 4 subplots (one per partition configuration) with:
  - Left Y axis (blue): Throughput (MB/s) with prediction curve
  - Right Y axis (red): Overhead (%) with prediction curve
  - Blue markers (+): Experimental throughput results
  - Red markers (○): Experimental overhead results

- **`Network bandwidth evaluation`**: 

  ![Network Bandwidth Evaluation](./experiments/network_bandwidth_evaluation/plots/network_bandwidth_evaluation.png)

---

### 5. Correlation Analysis
**Goal:** analyze the correlation between packet size (Sr) and send interval (Gs).
```bash
# Runs 10 runs x 56 experiments (4 partitions × 14 batch sizes)
./experiments/correlation_analysis/correlation_analysis_launch.sh

# Generate plot
python ./experiments/correlation_analysis/plot_correlation_analysis.py
```

**Configuration:**
  - Partition counts: 3, 6, 9, 12
  - Batch sizes: 10 30 50 70 100 120 150 170 190 210 230 250 280 300
  - Payload size: 1000 bytes
  - Number of runs per configuration: 10
  - Duration: 60s production + 30s warmup
  - Message generation mode: Heavy mode

**Average duration of the whole experiment:** ~23 hours

**Formulas:**

1. **Serialized Message Size (Sm):**
   
   **Sm** = $len(keyBytes) + len(valueBytes)$

2. **Serialized Batch Size (Sb):**
   
   **Sb** = $\frac{S_m \cdot N_b + batchHeaderSize}{compressionRatio}$

   **Where:**
   - `Nb` = number of messages per batch  
   - `batch_header_size` = 61 bytes (Kafka protocol)  
   - `compression_ratio` = 1.0 (no compression)

3. **Group Send Interval (Gs):**
   
   **Gs** = $timestampCurrentGroup - timestampPreviousGroup$

   A *group* consists of $P$ consecutive batches (where $P$ = partitions per broker).

4. **Request Size (Sr):**
   
   **Sr** = $S_b \cdot P + packetHeaderSize$

   **Where:**
   - `P` = number of partitions handled by a broker  
   - `packet_header_size` = TCP/IP + Kafka protocol overhead

5. **Packet Header Size:**
   
   **packet_header_size** = $\frac{kafkaTotalBytes - \sum (S_b \cdot numBatches)}{totalBatches}$

   Where `kafka_total_bytes` is collected via JMX (`BytesInPerSec`).


**Multi-run aggregation:**
For each configuration (partitions, batch size), the runs are aggregated with:
  - Outlier filter: Tukey IQR on Gs as anchor to exclude anomalous runs
  - Estimate: 95% confidence interval (t-distribution) on the filtered runs; the representative value is $\mathrm{representativePoint} = \frac{CI_{\mathrm{lower}} + CI_{\mathrm{upper}}}{2}$

**Plot:**
The plot shows 4 subplots (one per partition configuration) with:
  - Y axis: Packet Send Interval (ms)
  - X axis: Packet Size (KB)  
  - Linear fit: 
    **Gs** = $a \cdot S_r + b$
  - R² score to evaluate the correlation

- **`Correlation Analysis`**: 

  ![Correlation Analysis](./experiments/correlation_analysis/plots/correlation_analysis.png)

> **R²** (coefficient of determination) measures the proportion of the variance of the
> data explained by the regression model. It ranges from 0 to 1: a value close
> to 1 indicates that the model fits the data well; a low value indicates that
> the assumed relationship does not adequately capture the observed behaviour.

## 📊 Metrics and Output

### Output Files

- **`data/producer_*_messages.jsonl`**: Message send records
  ```json
  {
    "key": "8", 
    "run_id": "test_3_partitions_NB_50_PS_200", 
    "topic": "test_3_partitions", 
    "partition": 2,
    "broker_id": 1,
    "message": ".........", 
    "message_size_bytes": 411, 
    "send_timestamp_ms": 1768665395468.925,
    "status": "OK", 
    "send_batch_id": 1
  }
  ```

  | Field                | Description                                                   |
  |----------------------|---------------------------------------------------------------|
  | `key`                | Unique key of the message                                     |
  | `run_id`             | Unique experiment identifier						     |
  | `topic`              | Kafka topic the messages were sent to                         |
  | `partition`          | Destination Kafka partition                                   |
  | `broker_id`          | ID of the leader broker of the partition                      |
  | `message`            | Actual payload of the message             					 |
  | `message_size_bytes` | Total size (serialized key + value)                           |
  | `send_timestamp_ms`  | Creation timestamp of the message in the producer             |
  | `status`             | Delivery status of the message             				 |
  | `send_batch_id`      | ID of the batch the message belongs to                        |

- **`data/consumer_*_messages.jsonl`**: Message reception records
  ```json
  {
    "key": "1", 
    "run_id": "test_3_partitions_NB_50_PS_200", 
    "topic": "test_3_partitions", 
    "partition": 2, 
    "offset": 0, 
    "message": "..........", 
    "receive_timestamp_ms": 1768665395603.1628, 
    "producer_send_timestamp_ms": 1768665395455.4229
  }
  ```

  | Field                      | Description                                        					|
  | -------------------------- | ---------------------------------------------------------------------- |
  | `key`                        | Unique key of the Kafka message                    					|
  | `run_id`                     | Unique experiment identifier                       					|
  | `topic`                      | Kafka topic the messages were sent to               					|
  | `partition`                  | ID of the Kafka partition the consumer read the message from           |
  | `offset`                     | Offset of the message in the partition             					|
  | `message`                    | Actual payload of the message               							|
  | `receive_timestamp_ms`       | Reception timestamp in the consumer               					    |
  | `producer_send_timestamp_ms` | Send timestamp from the producer (for latency computation) 					|

- **`data/stats/producer_*_stats.json`**: Producer statistics
  ```json
  {
    "run_id": "test_3_partitions_NB_10_PS_200",
    "producer_name": "producer_1",
    "topic": "test_3_partitions",
    
    "transmission_stats": {
      "num_messages": 5290,
      "num_batches": 529,
      "payload_bytes": 2218758,
      "transmission_time_seconds": 20.01
    },
    
    "kafka_metrics": {
      "kafka_overhead_bytes": 173690,
      "kafka_total_bytes": 4602647,
      "throughput_kbps": 224.51,
      "overhead_percentage": 3.77,
      "packet_header_size_bytes": 103.07
    },
    
    "experiment_config": {
      "partition_count": 3,
      "nb_messages_per_batch": 10,
      "batch_header_size_bytes": 61,
      "compression_ratio": 1.0,
      "partition_to_broker": {
        "0": 2,
        "1": 3,
        "2": 1
      },
      "lambda_rate": 100.0,
      "linger_ms": 30000
    },
    
    "broker_tracking_summary": {
      "1": {
        "batch_count": 523,
        "partition_count": 1
      },
      "2": {
        "batch_count": 516,
        "partition_count": 1
      },
      "3": {
        "batch_count": 524,
        "partition_count": 1
      }
    },
    
    "batch_metrics": {
      "Nb": 10,
      "P": 1,
      "Sm_mean_bytes": 419.48,
      "Sb_mean_bytes": 4255.76,
      "Gs_mean_ms": 33.36,
      "Sr_mean_bytes": 4358.83
    }
  }
  ```
  | Section                 | Field                     | Description                                                									|
  | ----------------------- | ------------------------- | ----------------------------------------------------------------------------------------------|
  | `Root`                    | `run_id`                    | Unique experiment identifier                                                                  |
  |                         | `producer_name`             | Name of the producer container that generated the data                                        |
  |                         | `topic`                     | Kafka topic the messages were sent to                                                         |
  | `transmission_stats`      | `num_messages`              | Total number of messages sent by the producer                                                 |
  |                         | `num_batches`               | Total number of batches sent                                                                  |
  |                         | `payload_bytes`             | Total size of the payloads ONLY (useful content of the messages, without Kafka/TCP/IP headers) |
  |                         | `transmission_time_seconds` | Actual transmission time (from the first to the last message)                                 |
  | `kafka_metrics`           | `kafka_total_bytes`         | Total bytes received by the brokers (payload + Kafka overhead + TCP/IP headers)               |
  |                         | `kafka_overhead_bytes`      | Total overhead (Kafka + TCP/IP headers + metadata)                                            |
  |                         | `throughput_kbps`           | Actual throughput measured by the brokers                                                     |
  |                         | `overhead_percentage`       | Overhead percentage over the total                         									|
  |                         | `packet_header_size_bytes`  | Average network header size per packet (TCP/IP + Kafka protocol overhead)                     |
  | `experiment_config`       | `partition_count`           | Number of partitions of the topic                          									|
  |                         | `nb_messages_per_batch`     | Number of messages per batch (configured in the producer)                      				|
  |                         | `batch_header_size_bytes`   | Fixed size of the Kafka batch header                                   						|
  |                         | `compression_ratio`         | Compression ratio (1.0 = no compression)                   									|
  |                         | `partition_to_broker`       | Partition → leader broker mapping                          									|
  |                         | `lambda_rate`       | Lambda value for generation according to the Poisson process (0.0 in heavy mode)                         									|
  |                         | `linger_ms`       | Maximum waiting time before a batch is sent                                   									|
  | `broker_tracking_summary` | `batch_count`               | Number of batches received by the broker                   									|
  |                         | `partition_count`           | Number of partitions handled by the broker                 									|
  | `batch_metrics`           | `Nb`                        | Number of messages per batch                               									|
  |                         | `P`                         | Number of leader partitions per broker                            							|
  |                         | `Sm_mean_bytes`             | Average serialized message size (key + JSON value)          									|
  |                         | `Sb_mean_bytes`             | Average serialized batch size            														|
  |                         | `Gs_mean_ms`                | Average interval between sends of complete groups of batches (every P batches)               |
  |                         | `Sr_mean_bytes`             | Average network packet size (batch + TCP/IP headers)              							|

- **`experiments/latency_analysis/reports/latency_report.json`**: Generated during the Latency Analysis experiment
```json
{
  "run_id": "nb10_part3_r1",
  "mode": "continuous",
  "lambda": null,
  "nb": 10,
  "partitions": 3,
  "run_number": 1,
  "throughput_mbps": 4.82,
  "global": {
    "latency_min_ms": 21.738,
    "latency_max_ms": 6257.438,
    "latency_mean_ms": 4061.153,
    "latency_std_ms": 1602.097,
    "latency_p25_ms": 2795.458,
    "latency_p50_ms": 4414.311,
    "latency_p75_ms": 5421.539,
    "latency_p95_ms": 6080.554,
    "latency_p99_ms": 6204.995,
    "latency_iqr_ms": 2626.081,
    "message_count": 200000
  },
  "by_consumer": [
    {
      "consumer": "consumer_1_g1",
      "latency_min_ms": 21.738,
      "latency_max_ms": 6238.753,
      "latency_mean_ms": 4091.153,
      "latency_std_ms": 1608.831,
      "latency_p25_ms": 2815.45,
      "latency_p50_ms": 4528.89,
      "latency_p75_ms": 5462.952,
      "latency_p95_ms": 6116.023,
      "latency_p99_ms": 6206.054,
      "latency_iqr_ms": 2647.502,
      "message_count": 33544
    },
    {
      "consumer": "consumer_1_g2",
      "latency_min_ms": 22.621,
      "latency_max_ms": 6257.438,
      "latency_mean_ms": 4061.152,
      "latency_std_ms": 1602.069,
      "latency_p25_ms": 2795.166,
      "latency_p50_ms": 4414.636,
      "latency_p75_ms": 5420.5,
      "latency_p95_ms": 6080.517,
      "latency_p99_ms": 6204.945,
      "latency_iqr_ms": 2625.334,
      "message_count": 100000
    },
    {
      "consumer": "consumer_2_g1",
      "latency_min_ms": 25.188,
      "latency_max_ms": 6257.429,
      "latency_mean_ms": 4046.014,
      "latency_std_ms": 1598.539,
      "latency_p25_ms": 2785.179,
      "latency_p50_ms": 4374.741,
      "latency_p75_ms": 5397.569,
      "latency_p95_ms": 6063.849,
      "latency_p99_ms": 6192.665,
      "latency_iqr_ms": 2612.391,
      "message_count": 66456
    }
  ]
}
```

| Section       | Field               | Description                                                                 |
|---------------|---------------------|-----------------------------------------------------------------------------|
| `Root`        | `run_id`            | Unique identifier of the run                                                |
|               | `mode`              | Message generation mode (`poisson` / `continuous`)                          |
|               | `lambda`            | Lambda rate if `mode=poisson`, `null` if `mode=continuous`                  |
|               | `nb`                | Batch size used in the run                                                  |
|               | `partitions`        | Number of partitions of the topic                                           |
|               | `run_number`        | Progressive index of the run within the configuration                      |
|               | `throughput_mbps`   | Average throughput measured during the run (KB/s)                           |
| `global`      | `latency_min_ms`    | Minimum latency observed over all the messages of the run                   |
|               | `latency_max_ms`    | Maximum latency observed over all the messages of the run                   |
|               | `latency_mean_ms`   | Global mean latency of the run         |
|               | `latency_std_ms`    | Standard deviation of the latency                                           |
|               | `latency_p25_ms`    | 25th percentile of the latency distribution                                 |
|               | `latency_p50_ms`    | Median of the latency distribution                                          |
|               | `latency_p75_ms`    | 75th percentile of the latency distribution                                 |
|               | `latency_p95_ms`    | 95th percentile of the latency distribution                                 |
|               | `latency_p99_ms`    | 99th percentile of the latency distribution                                 |
|               | `latency_iqr_ms`    | Interquartile Range of the latency (`p75 - p25`)                            |
|               | `message_count`     | Total number of messages received by all the consumers in the run          |
| `by_consumer` | `consumer`          | Consumer name (`consumer_1_g1`, `consumer_2_g1`, `consumer_1_g2`)          |
|               | `latency_*`         | Same metrics as the `global` section, computed on the single consumer       |
|               | `message_count`     | Number of messages received by the single consumer                          |


## 🐳 Docker Compose

The `docker-compose.yml` file orchestrates:

- **3 Kafka Brokers** with KRaft 
- **Kafka UI** (port 8080) for cluster visualization
- **2 Producers** on a dedicated network
- **3 Consumers** on 2 distinct networks (to test separate emulation)
- **Network Emulator** on all networks

## ⚙️ Environment Variables

| Variable | Description 
|-----------|-------------
| `EXPERIMENT_DURATION` | Experiment duration (seconds) 
| `TARGET_MESSAGES` | Number of messages to generate 
| `TOPIC_NAME` | Kafka topic 
| `RUN_ID` | Run ID for tracking 
| `PAYLOAD_SIZE_BYTES` | Message payload size 
| `NB_MESSAGES_PER_BATCH` | Messages per batch
| `PARTITION_COUNT` | Number of partitions 
| `SEND_MODE` | Message generation mode of the producers
| `LAMBDA_RATE` | Lambda rate if SEND_MODE = POISSON
| `BOOTSTRAP_SERVERS` | Kafka bootstrap servers 

## 🧹 Cleanup

```bash
bash scripts/teardown_environment.sh
```

Stops the containers and cleans up the files (with an interactive prompt).

## 📌 Important Notes
1. **Replication Factor:** Hardcoded to 3 in all topics
2. **Single Topic:** The producers write to a single topic per experiment (no simultaneous multi-topic)
3. **JMX Requirement:** `update_stats_with_jmx.py` requires running brokers to collect metrics
4. **Batch and `linger_ms`:** In all the experiments the producer uses `linger_ms = 30000 ms`, a value high enough to guarantee that, under **heavy load**, batches are almost always sent at full capacity (Nb messages) and not because the timer expires. In this regime the system is **throughput-bound**: the message stream is enough to fill all the buffers quickly and `linger_ms` never becomes the bottleneck. In the **latency analysis in Poisson mode**, instead, we also explore configurations with very low λ and a high number of partitions. In these cases the effective rate per partition can be so low that the theoretical batch fill time exceeds `linger_ms`. When this happens, sending happens **on timeout** and not because the batch is full: the batches are therefore sent partially full and the observed latency mainly reflects the value of `linger_ms`. This is a distinct regime (input-bound / timeout-bound) that is discussed in the latency analysis section.



## 🔚 Conclusions
The experiments carried out on the three scenarios (latency analysis, correlation analysis and network bandwidth evaluation) highlight some structural behaviours of the Kafka system in the presence of batching and partitioning.

---

### Latency Analysis: effect of batch size and lambda rate on end-to-end latency

The **latency analysis** measures the end-to-end time between the producer sending the message and the consumer receiving it, as the batch size **(Nb)** and the lambda rate **(λ)** vary, in **Poisson** mode.

#### Effect of the batch size (Nb)

As Nb increases, an **almost linear increase in the mean latency** is observed. This behaviour is due to the fact that with larger batches the producer holds the messages in the buffer for longer before sending them, increasing the overall waiting
time. The linear regression estimated in the plots confirms this trend with high R² coefficients.
The effect is particularly marked for **low λ**: with low rates, messages arrive slowly and the buffer takes longer to fill, so even small increases in Nb translate into significant latency increases (lines with a steep
slope). This holds as long as lambda is able to fill the batches within linger.ms; after that, saturation is reached and the latency stabilizes. As λ grows, the batches fill up faster and the slope progressively decreases: the latency still grows linearly with Nb, but much more moderately.

#### Effect of the lambda rate (λ)

As λ increases, the latency **decreases** following a **power-hyperbolic** trend (fit on a log scale). With high rates, batches fill up faster, reducing the waiting time in the producer buffer. Beyond a certain threshold, the latency tends to stabilize and the system reaches a regime in which the dominant time is the broker's transmission and processing time, no longer the batch accumulation time.

For very low λ and many topic partitions (e.g. λ=10 with P=9 or 12) a different behaviour is observed: the effective rate per partition $\frac{\lambda}{P}$ can become so small that the time needed to fill a batch exceeds `linger_ms = 30000 ms`. In this regime, batches are often sent on timeout rather than because they are full; the latency is therefore bounded above by the value of `linger_ms` and stops following a "clean" hyperbolic law. These edge cases are kept in the results precisely to show the boundary between the batch-dominated regime and the timeout-dominated regime.

#### Effect of the number of partitions

The heatmaps and boxplots show that **doubling the number of partitions tends to double the mean latency**. This behaviour is due to the fact that with more partitions the producer keeps **more parallel internal buffers** (one per partition). For the same λ, the message stream is spread over a larger number of buffers, so each one takes longer to reach the fill threshold required for sending. The message therefore waits longer in the buffer before the batch is transmitted.

The **`latency_ratio_vs_partition_number`** plot shows how, in most configurations, doubling the number of partitions brings the latency close to a factor of ≈2×, confirming the proportional nature of the impact of partitioning on latency. The exception is the case of **low λ and high Nb**, where sending is dominated by `linger_ms` and not by the accumulation in the per-partition buffers, so adding partitions no longer multiplies the waiting time but leaves the latency  substantially unchanged.

In the **Poisson** regime, the results suggest some practices to optimize latency:
- Prefer small batch sizes (Nb) if the goal is low latency
- Avoid an excessive number of partitions, since it leads to higher latency
- Configure a **sufficiently high λ**: high rates reduce both the mean latency and its variability, bringing the system to a more stable and predictable regime


---

### Correlation Analysis: relationship between packet size and send times

In the **correlation analysis** plot it can be observed that, for the same configuration, the average send interval of the batch groups **(Gs)** grows as the average serialized packet size **(Sr)** increases. This behaviour is expected: larger packets take more time to be sent over the network and processed by the TCP/IP stack and by the broker, so the interval between two send "groups" tends to get longer.

However, the increase in time is not proportional (it does not exactly double when the packet size doubles), but it fits an **almost linear regression** of the form:

$$Gs \approx a \cdot Sr + b$$

where:
- The constant term **(b)** represents the **fixed cost per request** (protocol overhead, scheduling, etc.)
- The coefficient **(a)** is mainly related to the **available bandwidth** and to the **network latency**

In other words, there is a minimum "per request" cost that is paid regardless of the batch size, and a variable cost that grows with the bytes actually transmitted.

#### Effect of the number of partitions on Gs

Comparing the different partition configurations, it can be seen that **the send interval Gs increases with the number of partitions**. This behaviour is consistent with the internal architecture of the Kafka producer:

- **More partitions** → **more internal queues** in the producer (one per partition)
- Higher **concurrency** between send threads
- Increased **coordination overhead** (locks, synchronization, ...)
- More HTTP/TCP requests to handle simultaneously towards the brokers

The producer has to coordinate the sending towards a larger number of partitions spread over the brokers, which increases the time needed to complete a full send cycle. 
The net result is that the average send time per group grows with the number of partitions, despite benefiting from the parallelization on the broker side. The estimated linear regression lines in fact show **different slopes and intercepts** for each partition configuration, confirming this systemic effect.

---

### Network Bandwidth Evaluation: throughput and overhead as the batch size varies

In the **network bandwidth evaluation** plot, for a fixed number of partitions, the **throughput** grows as the batch size increases, but not **linearly**: the first increases in batch size bring a very marked gain, while beyond a certain threshold the throughput increase tends to saturate.

The throughput grows according to an **exponential saturation** model:

$$T(x) = c - a \cdot e^{-bx}$$

where:
- **x** is the batch size **(Nb)**
- **c** represents the **theoretical maximum throughput**
- **a** and **b** characterize the speed of saturation

**Initial phase (small batches):**  
The exponential term $e^{-bx}$ is large → low throughput.

**Saturation phase (large batches):**  
The exponential term tends to 0 → the throughput approaches **c**.

#### Trend of the relative overhead

As for the **percentage overhead**, the curve shows a **power-law decay**:

$$O(x) = a \cdot x^{-b} + c$$

The **stabilization** of the overhead at low values reflects the fact that, by further increasing the batch size, the structure of the Kafka protocol does not change.

### Effect of the number of partitions

Varying the number of partitions, **shifts and differences in shape** are observed in the throughput and overhead curves.

**More partitions → Advantages:**
- Greater parallelism
- Higher theoretical maximum throughput
- Better load distribution

**More partitions → Disadvantages:**
- More metadata to manage
- More replication traffic
- More internal queues in the producer
- Growing coordination overhead

There is therefore an **optimal region** that balances parallelism and overhead.
**Practical trade-off**: P=3-6 is optimal for most cases; P=12 only with high λ and small Nb.


## 📚 References

### 1. Performance Prediction Model

Wu, H., Shang, Z., & Wolter, K. (2019). *Performance Prediction for the Apache Kafka Messaging System*. In 2019 IEEE 21st International Conference on High Performance Computing and Communications (HPCC), pp. 154-161. IEEE.

**Link:** https://doi.org/10.1109/HPCC/SmartCity/DSS.2019.00036

### 2. Reactive Batching Strategy

Wu, H., Shang, Z., & Wolter, K. (2018). *A Reactive Batching Strategy of Apache Kafka for Reliable Stream Processing in Real-time*. In 2018 IEEE International Symposium on Software Reliability Engineering Workshops (ISSREW), pp. 207-214. IEEE.

**Link:** https://ieeexplore.ieee.org/document/9251089
