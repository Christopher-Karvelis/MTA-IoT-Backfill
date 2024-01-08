#!/bin/bash
# git pull for automatic intraday updates
git pull origin $(git branch --show-current)
git pull origin main