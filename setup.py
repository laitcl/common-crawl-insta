import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="common-crawl-insta",
    version="0.0.1",
    author="laitcl",
    author_email="laitcl@gmail.com",
    description="An instagram common-crawl project",
    long_description="""
    Reverse link search for instagram links
    """,
    long_description_content_type="text/markdown",
    url="https://github.com/laitcl/common-crawl-insta",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)