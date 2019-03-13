# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

## 1.0.1 - 2019-03-13

### Added
- handle Clowder's file removed message in geo extractors.
[BD-2205](https://opensource.ncsa.illinois.edu/jira/browse/BD-2205)

### Fixed
- Added Dockerfile to create geoserver and pycsw images.
- Showing worldwide map extent in WGS84 projection in geospatial viewer (industry project request)
- make data with unknown projection not processed in extractor (industry project request)
