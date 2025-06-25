    from setuptools import setup, find_packages
    setup(
            name='weaviate_local_gui',
            version='0.1.0',
            packages=find_packages(),
            install_requires=[
                'weaviate-client',
                'streamlit'
                # Add other dependencies here
            ],
            python_requires='>=3.11',
        )
