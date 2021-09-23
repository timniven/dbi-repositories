#!/bin/bash

git tag $(cat version)
git push origin --tags
