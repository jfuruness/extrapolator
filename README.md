[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-3100/)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)
![Tests](https://github.com/jfuruness/extrapolator/actions/workflows/tests.yml/badge.svg)

# extrapolator

* [Description](#package-description)
* [Usage](#usage)
* [Installation](#installation)
* [Testing](#testing)
* [Credits](#credits)
* [History](#history)
* [Development/Contributing](#developmentcontributing)
* [Licence](#license)


## Package Description

This package determines the real world accuracy of a BGP simulator

TODO: add dependencies properly

1. Download ROV information
2. Download MRT files and analyze using the MRTAnalysis repo
3. For each vantage point (defined as the AS that sends the collectors announcements, at the end of the AS Path), get:
    a. Total number of announcements
    b. Total number of prefixes
    c. AS Rank using the ASRankCollector repo
4. Remove vantage points that have way more announcements than prefixes
    a. These are likely to be RIB In rather than localRIB
5. Select the top 10 vantage points
6. Select the prefixes that exist at all the vantage points we want to cover
    a. Removing any prefixes that have signs of path poisoning
7. For each vantage point:
    a. Omit the vantage point
    b. Propagate announcements and write a CSV of announcements at the omitted vantage point
    c. Compare the ground truth dataset from the MRT RIB dumps to the propagated announcements
        i. Use levenshtien distance for this
8. Determine the statistical significance of these findings for all vantage points
9. Repeat steps 7 and 8 for each experiment we want to run
    a. For example: multihomed provider preferene
    b. For example: ROV
    c. For example: using prob link instead of CAIDA

## Usage
* [extrapolator](#extrapolator)

TODO

## Installation
* [extrapolator](#extrapolator)

Install python and pip if you have not already.

Then run:

```bash
pip3 install pip --upgrade
pip3 install wheel
```

For production:

```bash
pip3 install extrapolator
```

This will install the package and all of it's python dependencies.

If you want to install the project for development:
```bash
git clone https://github.com/jfuruness/extrapolator.git
cd extrapolator
pip3 install -e .[test]
pre-commit install
```

To test the development package: [Testing](#testing)


## Testing
* [extrapolator](#extrapolator)

To test the package after installation:

```
cd extrapolator
pytest extrapolator
ruff extrapolator
black extrapolator
mypy extrapolator
```

If you want to run it across multiple environments, and have python 3.10 and 3.11 installed:

```
cd extrapolator
tox
```

## Credits
* [extrapolator](#extrapolator)

This concept has been worked on for pretty much the entirety of my time at UConn.
It has changed many hands and been worked on by a lot of people.
I'll try to list as many as I can, apologies if I've forgotten someone,
please just let me know and I'd be happy to add you:

* Sam Secondo for his latest iteration of the BGPExtrapolator
* James Breslin for working on it prior to Sam, along with:
    * Cameron Morris
    * Reynaldo Morillo
* Michael Pappas for working on the original version

As well as all of the undergrads that have worked on various aspects of it:
* Matt Jaccino
* Tony Zheng
* Nicholas Shpetner
* and many more that I don't remember

Thanks as well to the professors that have employed me while working on this project:
Dr. Amir Herzberg
Dr. Bing Wang

And thanks to the variety of data sources that we have used for this.
Since these are credited in the various other repos, I'm going to omit them here


## History
* [extrapolator](#extrapolator)

TODO

## Development/Contributing
* [extrapolator](#extrapolator)

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Test it
5. Run tox
6. Commit your changes: `git commit -am 'Add some feature'`
7. Push to the branch: `git push origin my-new-feature`
8. Ensure github actions are passing tests
9. Email me at jfuruness@gmail.com if it's been a while and I haven't seen it

## License
* [extrapolator](#extrapolator)

BSD License (see license file)
