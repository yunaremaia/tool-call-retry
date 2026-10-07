# Contributing to tool-call-retry

Thank you for your interest in contributing! This document outlines how to get started.

## How to Contribute

1. **Fork the repository** and create a new branch for your changes.
2. **Make your changes** following the existing code style and conventions.
3. **Write tests** for any new functionality or bug fixes.
4. **Run the test suite** to ensure nothing is broken.
5. **Submit a Pull Request** with a clear description of the changes.

## Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/tool-call-retry.git
cd tool-call-retry

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -e .
# or
pip install -r requirements.txt
```

## Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=tool-call-retry

# Run specific test file
pytest tests/test_specific.py
```

## Submitting PRs

- Keep PRs focused on a single change
- Write clear commit messages
- Update documentation if needed
- Ensure CI passes before requesting review
- Be responsive to feedback

## Code Style

- Follow PEP 8 for Python code
- Use meaningful variable names
- Add docstrings to public functions and classes
- Keep functions small and focused

## Reporting Issues

- Use the GitHub issue tracker
- Include a minimal reproducible example
- Specify your environment (OS, Python version, etc.)

Thank you for contributing!
