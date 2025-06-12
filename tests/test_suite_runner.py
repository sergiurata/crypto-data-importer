"""
Test Suite Runner for Crypto Data Importer
Comprehensive test runner with coverage reporting and test organization
"""

import unittest
import sys
import os
import argparse
from io import StringIO
import time
from typing import List, Dict, Any
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

# Optional imports for enhanced features
try:
    import coverage
    COVERAGE_AVAILABLE = True
except ImportError:
    COVERAGE_AVAILABLE = False

try:
    from xmlrunner import XMLTestRunner
    XML_RUNNER_AVAILABLE = True
except ImportError:
    XML_RUNNER_AVAILABLE = False


class ColoredTextTestResult(unittest.TextTestResult):
    """Enhanced test result with colored output"""
    
    def __init__(self, stream, descriptions, verbosity):
        super().__init__(stream, descriptions, verbosity)
        self.test_start_time = None
        self.use_colors = hasattr(stream, 'isatty') and stream.isatty()
    
    def _colored(self, text: str, color: str) -> str:
        """Apply color to text if terminal supports it"""
        if not self.use_colors:
            return text
        
        colors = {
            'green': '\033[92m',
            'red': '\033[91m',
            'yellow': '\033[93m',
            'blue': '\033[94m',
            'purple': '\033[95m',
            'cyan': '\033[96m',
            'white': '\033[97m',
            'bold': '\033[1m',
            'end': '\033[0m'
        }
        
        return f"{colors.get(color, '')}{text}{colors['end']}"
    
    def startTest(self, test):
        super().startTest(test)
        self.test_start_time = time.time()
        if self.verbosity > 1:
            self.stream.write(f"  {test._testMethodName} ... ")
            self.stream.flush()
    
    def addSuccess(self, test):
        super().addSuccess(test)
        if self.verbosity > 1:
            elapsed = time.time() - self.test_start_time
            self.stream.writeln(self._colored(f"ok ({elapsed:.3f}s)", 'green'))
    
    def addError(self, test, err):
        super().addError(test, err)
        if self.verbosity > 1:
            elapsed = time.time() - self.test_start_time
            self.stream.writeln(self._colored(f"ERROR ({elapsed:.3f}s)", 'red'))
    
    def addFailure(self, test, err):
        super().addFailure(test, err)
        if self.verbosity > 1:
            elapsed = time.time() - self.test_start_time
            self.stream.writeln(self._colored(f"FAIL ({elapsed:.3f}s)", 'red'))
    
    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        if self.verbosity > 1:
            self.stream.writeln(self._colored(f"skipped '{reason}'", 'yellow'))


class ColoredTextTestRunner(unittest.TextTestRunner):
    """Test runner with colored output"""
    
    resultclass = ColoredTextTestResult
    
    def _makeResult(self):
        return self.resultclass(self.stream, self.descriptions, self.verbosity)


class TestSuiteRunner:
    """Main test suite runner with various options"""
    
    def __init__(self):
        self.test_dir = Path(__file__).parent
        self.coverage_instance = None
        if COVERAGE_AVAILABLE:
            self.coverage_instance = coverage.Coverage(
                source=[str(project_root / "src")],
                omit=[
                    'test_*.py',
                    'tests/*',
                    'venv/*',
                    '*/site-packages/*'
                ]
            )
    
    def discover_tests(self, pattern: str = None, start_dir: str = None) -> unittest.TestSuite:
        """Discover and load tests using unittest discovery"""
        loader = unittest.TestLoader()
        
        # Set default values
        if start_dir is None:
            start_dir = str(self.test_dir)
        if pattern is None:
            pattern = 'test_*.py'
        
        print(f"Discovering tests in: {start_dir}")
        print(f"Using pattern: {pattern}")
        
        try:
            suite = loader.discover(start_dir, pattern=pattern, top_level_dir=str(project_root))
            print(f"Found {suite.countTestCases()} test cases")
            return suite
        except Exception as e:
            print(f"Error during test discovery: {e}")
            return unittest.TestSuite()
    
    def run_specific_tests(self, test_names: List[str]) -> unittest.TestSuite:
        """Run specific test classes or methods"""
        suite = unittest.TestSuite()
        loader = unittest.TestLoader()
        
        for test_name in test_names:
            try:
                if '.' in test_name:
                    # Handle module.class.method format
                    parts = test_name.split('.')
                    if len(parts) >= 2:
                        module_name = parts[0]
                        class_name = parts[1]
                        
                        # Try to import the module
                        try:
                            module = __import__(module_name, fromlist=[class_name])
                            test_class = getattr(module, class_name)
                            
                            if len(parts) == 3:
                                # Specific test method
                                method_name = parts[2]
                                suite.addTest(test_class(method_name))
                            else:
                                # All tests in class
                                suite.addTest(loader.loadTestsFromTestCase(test_class))
                        except (ImportError, AttributeError) as e:
                            print(f"Could not load {test_name}: {e}")
                else:
                    # Try to find test class by name in discovered tests
                    all_tests = self.discover_tests()
                    found = False
                    
                    for test_group in all_tests:
                        if hasattr(test_group, '_tests'):
                            for test_case in test_group._tests:
                                if hasattr(test_case, '_tests'):
                                    for test in test_case._tests:
                                        if test.__class__.__name__ == test_name:
                                            suite.addTest(loader.loadTestsFromTestCase(test.__class__))
                                            found = True
                                            break
                                elif test_case.__class__.__name__ == test_name:
                                    suite.addTest(loader.loadTestsFromTestCase(test_case.__class__))
                                    found = True
                                    break
                    
                    if not found:
                        print(f"Warning: Test '{test_name}' not found")
            
            except Exception as e:
                print(f"Error loading test '{test_name}': {e}")
        
        return suite
    
    def list_all_tests(self) -> List[str]:
        """List all available tests"""
        tests = []
        suite = self.discover_tests()
        
        def extract_tests(test_suite):
            for test in test_suite:
                if isinstance(test, unittest.TestSuite):
                    extract_tests(test)
                else:
                    test_id = test.id()
                    tests.append(test_id)
        
        extract_tests(suite)
        return tests
    
    def run_tests(self, 
                  verbosity: int = 2,
                  pattern: str = None,
                  start_dir: str = None,
                  specific_tests: List[str] = None,
                  xml_output: str = None,
                  coverage_report: bool = False,
                  coverage_html: str = None,
                  fail_fast: bool = False) -> unittest.TestResult:
        """Run the test suite with specified options"""
        
        # Start coverage if requested
        if coverage_report and self.coverage_instance:
            self.coverage_instance.start()
        
        # Load tests
        if specific_tests:
            suite = self.run_specific_tests(specific_tests)
        else:
            suite = self.discover_tests(pattern, start_dir)
        
        # Check if we found any tests
        test_count = suite.countTestCases()
        if test_count == 0:
            print("No tests found!")
            print(f"Search directory: {start_dir or self.test_dir}")
            print(f"Search pattern: {pattern or 'test_*.py'}")
            return unittest.TestResult()
        
        # Choose runner
        if xml_output and XML_RUNNER_AVAILABLE:
            runner = XMLTestRunner(
                output=xml_output,
                verbosity=verbosity
            )
        else:
            runner = ColoredTextTestRunner(
                verbosity=verbosity,
                failfast=fail_fast,
                buffer=True
            )
        
        # Run tests
        print(f"Running {test_count} tests...")
        print("=" * 70)
        
        start_time = time.time()
        result = runner.run(suite)
        end_time = time.time()
        
        # Stop coverage and generate report
        if coverage_report and self.coverage_instance:
            self.coverage_instance.stop()
            self.coverage_instance.save()
            
            print("\n" + "=" * 70)
            print("COVERAGE REPORT")
            print("=" * 70)
            self.coverage_instance.report()
            
            if coverage_html:
                print(f"\nGenerating HTML coverage report: {coverage_html}")
                self.coverage_instance.html_report(directory=coverage_html)
        
        # Print summary
        self._print_summary(result, end_time - start_time)
        
        return result
    
    def _print_summary(self, result: unittest.TestResult, execution_time: float):
        """Print test execution summary"""
        print("\n" + "=" * 70)
        print("TEST SUMMARY")
        print("=" * 70)
        
        total_tests = result.testsRun
        failures = len(result.failures)
        errors = len(result.errors)
        skipped = len(result.skipped) if hasattr(result, 'skipped') else 0
        passed = total_tests - failures - errors - skipped
        
        print(f"Tests run: {total_tests}")
        print(f"Passed: {passed}")
        print(f"Failed: {failures}")
        print(f"Errors: {errors}")
        print(f"Skipped: {skipped}")
        print(f"Execution time: {execution_time:.2f} seconds")
        
        if failures > 0:
            print(f"\nFAILURES ({failures}):")
            for test, traceback in result.failures:
                print(f"  - {test}")
        
        if errors > 0:
            print(f"\nERRORS ({errors}):")
            for test, traceback in result.errors:
                print(f"  - {test}")
        
        success_rate = (passed / total_tests * 100) if total_tests > 0 else 0
        print(f"\nSuccess rate: {success_rate:.1f}%")
        
        if result.wasSuccessful():
            print("🎉 All tests passed!")
        else:
            print("❌ Some tests failed!")


def main():
    """Main entry point for test runner"""
    parser = argparse.ArgumentParser(description='Crypto Data Importer Test Suite')
    
    parser.add_argument('-v', '--verbosity', type=int, default=2, choices=[0, 1, 2],
                        help='Test output verbosity (0=quiet, 1=normal, 2=verbose)')
    
    parser.add_argument('-p', '--pattern', type=str, default=None,
                        help='Test discovery pattern (e.g., "test_*.py")')
    
    parser.add_argument('-s', '--start-dir', type=str, default=None,
                        help='Directory to start test discovery')
    
    parser.add_argument('-t', '--tests', nargs='+', default=None,
                        help='Specific tests to run (e.g., TestConfigurationManager test_init)')
    
    parser.add_argument('--xml', type=str, default=None,
                        help='Generate XML test reports in specified directory')
    
    parser.add_argument('--coverage', action='store_true',
                        help='Generate coverage report')
    
    parser.add_argument('--coverage-html', type=str, default=None,
                        help='Generate HTML coverage report in specified directory')
    
    parser.add_argument('--fail-fast', action='store_true',
                        help='Stop on first failure')
    
    parser.add_argument('--list-tests', action='store_true',
                        help='List all available tests')
    
    parser.add_argument('--integration', action='store_true',
                        help='Include integration tests (requires network)')
    
    args = parser.parse_args()
    
    # Set environment variable for integration tests
    if not args.integration:
        os.environ['SKIP_INTEGRATION_TESTS'] = '1'
    
    runner = TestSuiteRunner()
    
    if args.list_tests:
        # List all available tests
        print("Discovering tests...")
        tests = runner.list_all_tests()
        if tests:
            print(f"Found {len(tests)} tests:")
            for test in sorted(tests):
                print(f"  {test}")
        else:
            print("No tests found!")
            print(f"Search directory: {runner.test_dir}")
            print("Make sure your test files follow the naming pattern 'test_*.py'")
        return
    
    # Check for coverage availability
    if args.coverage and not COVERAGE_AVAILABLE:
        print("Warning: coverage.py not available. Install with: pip install coverage")
        args.coverage = False
    
    # Check for XML runner availability
    if args.xml and not XML_RUNNER_AVAILABLE:
        print("Warning: xmlrunner not available. Install with: pip install xmlrunner")
        args.xml = None
    
    # Run tests
    result = runner.run_tests(
        verbosity=args.verbosity,
        pattern=args.pattern,
        start_dir=args.start_dir,
        specific_tests=args.tests,
        xml_output=args.xml,
        coverage_report=args.coverage,
        coverage_html=args.coverage_html,
        fail_fast=args.fail_fast
    )
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == '__main__':
    main()
