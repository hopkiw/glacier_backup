from setuptools import setup

setup(
    name='glacier_backup',  # Required
    version='1.0',  # Required
    description='A Python client for performing backups to AWS S3 Glacier',
    url='https://github.com/hopkiw/glacier_backup',  # Optional
    author='Liam Hopkins',  # Optional
    classifiers=[  # Optional
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3 :: Only',
    ],
    packages=['glacier_backup'],
    python_requires='>=3.6, <4',
    package_data={  # Optional
        'glacier_backup.conf': ['glacier_backup.conf'],
    },
    entry_points={  # Optional
        'console_scripts': [
            'glacier_backup=glacier_backup.backup:main',
        ],
    },
)
