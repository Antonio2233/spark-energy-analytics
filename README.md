# Electricity demand, generation mix and price in Spain with Spark

Final project for the Big Data course (UMA Master's). We combine Red Eléctrica's data (demand, generation by technology and price) with weather and the natural gas price. With Spark ML we explain the hourly demand, generation mix and price from the 2021–2026 history. With Structured Streaming we follow the system in real time and raise alerts when it departs from what is expected.

## Documentation

- [Project plan](docs/plan.md): questions, sources, models, streaming and schedule by iteration.
- [AGENTS.md](AGENTS.md): instructions for AI-assisted development, Spark usage, Python guide and tests.

## Requirements

- [uv](https://docs.astral.sh/uv/) to manage Python and dependencies.
- Java 17 or 21, required by Spark 4.

## Getting started

```bash
uv sync
uv run pytest
```

## Layout

- `src/spain_electricity/`: package code.
- `docs/`: plan and documentation.
- `data/`: downloaded and generated data; not versioned.
