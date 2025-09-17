# Examples

This page provides an overview of the examples available in the `dagster-ray` repository. Each example demonstrates different aspects of integrating Ray with Dagster.

## Getting Started

All examples are located in the [`examples/`](https://github.com/danielgafni/dagster-ray/tree/main/examples) directory of the repository. To run these examples:

1. Clone the repository:
   ```bash
   git clone https://github.com/danielgafni/dagster-ray.git
   cd dagster-ray
   ```

2. Install dependencies:
   ```bash
   uv sync --all-extras
   ```

3. Navigate to the examples directory:
   ```bash
   cd examples
   ```

## Example Categories

### Basic Ray Integration

These examples demonstrate the fundamental concepts of using Ray with Dagster.

- **Local Ray Setup**: Basic examples using `LocalRay` resource for development
- **Ray IO Manager**: Examples showing how to use Ray's object store for intermediate data
- **Simple Distributed Compute**: Basic distributed computations using Ray remotes

### Executors and Launchers

Examples showing how to use Ray for executing Dagster jobs and individual ops.

- **Ray Executor**: Using `ray_executor` to distribute individual ops across Ray workers
- **Ray Run Launcher**: Submitting entire Dagster runs as Ray jobs
- **Mixed Execution**: Combining different execution strategies

### Pipes Integration

Examples demonstrating Dagster Pipes with Ray for external script execution.

- **Basic Pipes**: Simple external Ray script execution with logging
- **Complex Workflows**: Multi-step Ray pipelines with rich metadata
- **Error Handling**: Robust error handling and retry patterns

### KubeRay Examples

Kubernetes-specific examples using KubeRay for cluster management.

- **Basic KubeRay**: Getting started with KubeRay clusters
- **Auto-scaling**: Dynamic cluster scaling based on workload
- **GPU Workloads**: Examples using GPU resources in Ray clusters
- **Interactive Jobs**: Using KubeRay interactive mode
- **Job Mode**: Batch processing with KubeRay jobs

### Advanced Patterns

More sophisticated examples for production use cases.

- **Multi-Resource**: Using multiple Ray resources in the same pipeline
- **Conditional Execution**: Dynamic resource selection based on runtime conditions
- **Data Pipeline**: End-to-end data processing pipeline
- **ML Workflows**: Machine learning training and inference pipelines
- **Monitoring**: Comprehensive logging and monitoring setups

## Running Specific Examples

Each example directory contains:

- `README.md`: Specific instructions and explanation
- Python files with the Dagster definitions
- Configuration files (if needed)
- Requirements or setup scripts

### Local Development

For local development, most examples can be run with:

```bash
# From the example directory
dagster dev
```

### Kubernetes Examples

KubeRay examples require:

1. A Kubernetes cluster with KubeRay Operator installed
2. Proper RBAC permissions
3. Ray cluster images available

Refer to each example's README for specific requirements.

## Example Structure

Each example follows a consistent structure:

```
examples/
├── basic-ray/
│   ├── README.md
│   ├── definitions.py
│   └── requirements.txt
├── kuberay-autoscaling/
│   ├── README.md
│   ├── definitions.py
│   ├── ray-cluster.yaml
│   └── requirements.txt
└── ...
```

## Contributing Examples

We welcome contributions of new examples! When adding examples:

1. Include a comprehensive README explaining the use case
2. Add appropriate comments in the code
3. Test the example thoroughly
4. Consider both local and Kubernetes variants where applicable

## Troubleshooting

Common issues when running examples:

- **Missing Dependencies**: Ensure all required packages are installed
- **Kubernetes Access**: Verify cluster access and KubeRay operator status
- **Resource Limits**: Check if your cluster has sufficient resources
- **Network Issues**: Ensure Ray head node is accessible

For more help:

- Check the [Tutorial](tutorial/index.md) for detailed explanations
- Review the [API Reference](api.md) for specific configuration options
- Open an issue on [GitHub](https://github.com/danielgafni/dagster-ray/issues)

## Next Steps

After exploring the examples:

1. Try modifying examples to fit your use case
2. Review the [API documentation](api.md) for advanced configuration
3. Consider contributing your own examples back to the project
