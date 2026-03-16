# Part 1 - Kafka Simulator

## Setup

### 1. Install dependencies

From the **project root directory**:

```bash
pip install -r requirements.txt
```

### 2. Run the simulator

From the `students_part_1/` directory, in a **separate terminal** (with the venv activated):

```bash
cd students_part_1
python simulator.py
```

The simulator connects to Kafka at `localhost:9092` and continuously produces messages to 3 topics: `intel`, `attack`, and `damage`.

Keep the simulator running in the background while you work on your consumer.

Stop it with `Ctrl+C` when done.


## Files you get

| File                   | Description                   |
|------------------------|-------------------------------|
| `requirements.txt`     | Python dependencies           |
| `simulator.py`         | Produces messages to Kafka    |
| `haversine.py`         | Distance calculation function |
