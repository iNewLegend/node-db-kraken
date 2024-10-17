#!/bin/bash

# Check if Zsh is installed
if command -v zsh > /dev/null 2>&1; then
  echo "Zsh is installed. Running install-java.sh with Zsh..."
  /bin/zsh ./scripts/install-java-zsh.sh
else
  echo "Zsh is not installed. Running install-java.sh with Bash..."
  /bin/bash ./scripts/install-java.sh
fi
