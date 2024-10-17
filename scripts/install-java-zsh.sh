#!/bin/zsh
REQUIRED_JAVA_VERSION=17

# Function to get installed Java version
get_java_version() {
  java -version 2>&1 | awk -F '[ "]' '/version/ {print $4}'
}

# Function to compare versions
version_ge() {
  [ "$1" = "$(printf '%s\n' "$1" "$2" | sort -V | head -n1)" ]
}

is_java_in_path() {
  if [[ -n "$JAVA_HOME" && ":$PATH:" == *":$JAVA_HOME/bin:"* ]]; then
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
  if ! javac -version &> /dev/null; then
    return 1
  fi

  # Create a simple Java program to test compilation
  echo 'class Test { public static void main(String[] args) { System.out.println("Java is working!"); } }' > Test.java

  # Attempt to compile the simple Java program
  if ! javac Test.java &> /dev/null; then
    rm -f Test.java
    return 1
  fi

  # Clean up the test files
  rm -f Test.java Test.class

  # Check installed Java version
  java_version=$(get_java_version)
  if [[ -z "$java_version" ]]; then
    echo "Java is not installed. Attempting to install Java 17..."
    return 0
  fi

  echo "Java version $java_version is installed."

  if version_ge "$java_version" "$REQUIRED_JAVA_VERSION"; then
    echo "Java 17 or newer is already installed."
    return 1
  else
    echo "Java version is less than 17. Attempting to install Java 17..."
    return 0
  fi
}

# The 'brew' command must be available
if ! type brew &> /dev/null ; then
  echo "Homebrew is not installed. Please install Homebrew first."
  exit 1
fi

# Check if Java is already installed and functional
if is_java_functional; then
  echo "Java $REQUIRED_JAVA_VERSION or higher is already installed and functional."
  exit 0
else
  echo "Java $REQUIRED_JAVA_VERSION is not installed or not functional. Installing openjdk@$REQUIRED_JAVA_VERSION..."
  brew install openjdk@$REQUIRED_JAVA_VERSION
  brew link openjdk@$REQUIRED_JAVA_VERSION

  sleep 1

  if is_java_in_path; then
    echo "Java path located"
  else
    echo 'export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"' >> ~/.zshrc
    source ~/.zshrc
  fi


  # Ensure the installation was successful
  if is_java_functional; then
    echo "Java $REQUIRED_JAVA_VERSION installed successfully and is functional."
    exit 0
  else
    echo "Java installation failed. Please check the Homebrew logs."
    exit 1
  fi
fi
