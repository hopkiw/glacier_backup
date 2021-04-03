from setuptools import find_packages, setup

setup(
    name='glacier-backup',
    version='1.1',
    description='A Python client for performing backups to AWS S3 Glacier',
    long_description='A Python client for performing backups to AWS S3 Glacier',
    url='https://github.com/hopkiw/glacier_backup',
    author='Liam Hopkins',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3 :: Only',
    ],
    packages=find_packages('src'),
    package_dir={'': 'src'},
    python_requires='>=3.6, <4',
    install_requires=[
        'botocore>=1.12.103',
    ],
    package_data={
        'glacier_backup.conf': ['glacier_backup.conf'],
    },
    entry_points={
        'console_scripts': [
            'glacier-backup=glacier_backup.backup:main',
            'glacier-upload=glacier_backup.upload:main',
        ],
    },
)
