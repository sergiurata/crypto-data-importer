#!/bin/bash

# Crypto Data Importer Installation Script
# Supports Windows (Git Bash/WSL), Linux, and macOS

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running on Windows
is_windows() {
    [[ "$OSTYPE" == "cygwin" ]] || [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]
}

# Check if Python is installed
check_python() {
    log_info "Checking Python installation..."
    
    if command -v python3 &> /dev/null; then
        PYTHON_CMD="python3"
    elif command -v python &> /dev/null; then
        PYTHON_CMD="python"
    else
        log_error "Python is not installed or not in PATH"
        log_info "Please install Python 3.8+ from https://python.org"
        exit 1
    fi
    
    # Check Python version
    PYTHON_VERSION=$($PYTHON_CMD --version 2>&1 | awk '{print $2}')
    PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
    PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)
    
    if [[ $PYTHON_MAJOR -lt 3 ]] || [[ $PYTHON_MAJOR -eq 3 && $PYTHON_MINOR -lt 8 ]]; then
        log_error "Python 3.8+ is required. Found: $PYTHON_VERSION"
        exit 1
    fi
    
    log_success "Python $PYTHON_VERSION found"
}

# Check if pip is installed
check_pip() {
    log_info "Checking pip installation..."
    
    if command -v pip3 &> /dev/null; then
        PIP_CMD="pip3"
    elif command -v pip &> /dev/null; then
        PIP_CMD="pip"
    else
        log_error "pip is not installed"
        log_info "Please install pip: https://pip.pypa.io/en/stable/installation/"
        exit 1
    fi
    
    log_success "pip found"
}

# Create virtual environment
create_venv() {
    log_info "Creating virtual environment..."
    
    if [[ -d "venv" ]]; then
        log_warning "Virtual environment already exists"
        read -p "Do you want to recreate it? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            rm -rf venv
        else
            log_info "Using existing virtual environment"
            return
        fi
    fi
    
    $PYTHON_CMD -m venv venv
    log_success "Virtual environment created"
}

# Activate virtual environment
activate_venv() {
    log_info "Activating virtual environment..."
    
    if is_windows; then
        source venv/Scripts/activate
    else
        source venv/bin/activate
    fi
    
    log_success "Virtual environment activated"
}

# Install dependencies
install_dependencies() {
    log_info "Installing dependencies..."
    
    # Upgrade pip first
    $PIP_CMD install --upgrade pip
    
    # Install requirements
    if [[ -f "requirements.txt" ]]; then
        $PIP_CMD install -r requirements.txt
        log_success "Dependencies installed from requirements.txt"
    else
        log_warning "requirements.txt not found, installing basic dependencies"
        $PIP_CMD install requests pandas configparser
        
        # Install Windows-specific dependencies
        if is_windows; then
            $PIP_CMD install pywin32
        fi
    fi
    
    # Install development dependencies if requested
    if [[ "$INSTALL_DEV" == "true" ]]; then
        log_info "Installing development dependencies..."
        $PIP_CMD install pytest pytest-cov coverage black flake8 mypy xmlrunner
        log_success "Development dependencies installed"
    fi
}

# Install package in development mode
install_package() {
    log_info "Installing package in development mode..."
    
    if [[ -f "setup.py" ]]; then
        $PIP_CMD install -e .
        log_success "Package installed in development mode"
    else
        log_warning "setup.py not found, skipping package installation"
    fi
}

# Create sample configuration
create_config() {
    log_info "Creating sample configuration..."
    
    if [[ ! -f "config.ini" ]]; then
        if [[ -f "examples/sample_config.ini" ]]; then
            cp examples/sample_config.ini config.ini
            log_success "Sample configuration created: config.ini"
            log_info "Please edit config.ini with your settings"
        else
            log_warning "Sample configuration not found"
            log_info "Run 'python main.py create-config' to create one"
        fi
    else
        log_info "Configuration file already exists"
    fi
}

# Run tests to verify installation
run_tests() {
    log_info "Running tests to verify installation..."
    
    if [[ -f "tests/test_suite_runner.py" ]]; then
        $PYTHON_CMD tests/test_suite_runner.py -v 1
        log_success "Tests completed successfully"
    else
        log_warning "Test suite not found, skipping tests"
    fi
}

# Display post-installation instructions
show_instructions() {
    log_success "Installation completed successfully!"
    echo
    echo "Next steps:"
    echo "1. Edit config.ini with your AmiBroker database path and preferences"
    echo "2. Run the application:"
    
    if is_windows; then
        echo "   venv\\Scripts\\activate"
    else
        echo "   source venv/bin/activate"
    fi
    
    echo "   python main.py"
    echo
    echo "Available commands:"
    echo "   python main.py create-config    # Create configuration file"
    echo "   python main.py validate-config  # Validate configuration"
    echo "   python main.py status           # Show system status"
    echo "   python main.py help             # Show all commands"
    echo
    echo "For more information, see the README.md file"
}

# Main installation function
main() {
    echo "Crypto Data Importer - Installation Script"
    echo "=========================================="
    echo
    
    # Parse command line arguments
    INSTALL_DEV="false"
    RUN_TESTS="false"
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --dev)
                INSTALL_DEV="true"
                log_info "Development mode enabled"
                shift
                ;;
            --test)
                RUN_TESTS="true"
                log_info "Will run tests after installation"
                shift
                ;;
            --help)
                echo "Usage: $0 [options]"
                echo "Options:"
                echo "  --dev     Install development dependencies"
                echo "  --test    Run tests after installation"
                echo "  --help    Show this help message"
                exit 0
                ;;
            *)
                log_warning "Unknown option: $1"
                shift
                ;;
        esac
    done
    
    # Check system requirements
    check_python
    check_pip
    
    # Create and activate virtual environment
    create_venv
    activate_venv
    
    # Install dependencies and package
    install_dependencies
    install_package
    
    # Create sample configuration
    create_config
    
    # Run tests if requested
    if [[ "$RUN_TESTS" == "true" ]]; then
        run_tests
    fi
    
    # Show next steps
    show_instructions
}

# Run main function
main "$@"
