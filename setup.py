from setuptools import setup, find_packages

setup(
  name='parquet_checker',
  version='0.1.0',
  description='A Python library for checking parquet files',
  packages=find_packages(),
  include_package_data=True,
  entry_points={
      'console_scripts': [
          'parquet_checker = parquet_checker.parquet_checker:main'
      ]
  }
)
