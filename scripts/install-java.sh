#!/bin/sh
REQUIRED_JAVA_VERSION=17

# Function to get installed Java version
get_java_version() {
  java -version 2>&1 | awk -F '[ "]' '/version/ {print $4}'
}

# Function to compare versions
version_ge() {
  printf '%s\n' "$@" | sort -rV | head -n1 | grep -q "^$1$"
}

is_java_in_path() {
  if [ -n "$JAVA_HOME" ] && echo "$PATH" | grep -q "$JAVA_HOME/bin"; then
    echo "Java is in PATH."
    return 0
  else
    echo "Java is NOT in PATH."
    return 1
  fi
}

# Function to check if javac is properly installed and functional
is_java_functional() {
  # Check if javac command is available and functional
  if ! javac -version > /dev/null 2>&1; then
    return 1
  fi

  # Create a simple Java program to test compilation
  echo 'class Test { public static void main(String[] args) { System.out.println("Java is working!"); } }' > Test.java

  # Attempt to compile the simple Java program
  if ! javac Test.java > /dev/null 2>&1; then
    rm -f Test.java
    return 1
  fi

  # Clean up the test files
  rm -f Test.java Test.class

  # Check installed Java version
  java_version=$(get_java_version)
  if [ -z "$java_version" ]; then
    echo "Java is not installed. Attempting to install Java $REQUIRED_JAVA_VERSION..."
    return 0
  fi

  echo "Java version $java_version is installed."

  if version_ge "$java_version" "$REQUIRED_JAVA_VERSION"; then
    echo "Java $REQUIRED_JAVA_VERSION or newer is already installed."
    return 1
  else
    echo "Java version is less than $REQUIRED_JAVA_VERSION. Attempting to install Java $REQUIRED_JAVA_VERSION..."
    return 0
  fi
}

# Check if Java is already installed and functional
if is_java_functional; then
  echo "Java $REQUIRED_JAVA_VERSION or higher is already installed and functional."
  exit 0
else
  echo "Java $REQUIRED_JAVA_VERSION is not installed or not functional. Installing openjdk-$REQUIRED_JAVA_VERSION..."

  # The 'apt' command must be available
  if ! type apt > /dev/null 2>&1; then
    echo "apt is not installed. Please install apt first."
    exit 1
  fi

  sudo apt update
  sudo apt install -y openjdk-$REQUIRED_JAVA_VERSION-jdk

  # Ensure the installation was successful
  if is_java_functional; then
    echo "Java $REQUIRED_JAVA_VERSION installed successfully and is functional."
    exit 0
  else
    echo "Java installation failed. Please check the apt logs."
    exit 1
  fi
fi
