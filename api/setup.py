from setuptools import setup

setup(name='mlfdb',
      version='0.1',
      description='Machine learning feature db API',
      long_description='Machine learning feature db API',
      classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3'
      ],
      keywords='ml postgresql',
      url='http://github.com/fmidev/ml_feature_db',
      author='Roope Tervo',
      author_email='roope.tervo@fmi.fi',
      license='MIT',
      packages=['mlfdb'],
      install_requires=[
          'psycopg2-binary',
          'configparser',
          'numpy'
      ],
      include_package_data=True,
      zip_safe=False)
