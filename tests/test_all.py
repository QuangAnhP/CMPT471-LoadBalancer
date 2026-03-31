import unittest
import os
import sys

if __name__ == "__main__":
    # Discover and run all test_*.py files in this directory
    test_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, test_dir)
    loader = unittest.TestLoader()
    suite = loader.discover(test_dir, pattern="test_*.py")
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    # Exit with appropriate code
    sys.exit(not result.wasSuccessful())
