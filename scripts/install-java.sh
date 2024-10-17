#!/bin/bash

# Function to check if javac is properly installed and functional
is_java_functional() {
    # Check if javac command is available and functional
    if ! javac -version &> /dev/null; then
        return 1
    fi

    # Create a simple Java program to test compilation
    echo 'class Test { public static void main(String[] args) { System.out.println("Java is working!"); } }' > Test.java

    # Attempt to compile the simple Java program
    javac Test.java &> /dev/null
    if [[ $? -ne 0 ]]; then
        rm -f Test.java
        return 1
    fi

    # Clean up the test files
    rm -f Test.java Test.class

    return 0
}

# Check if Java is already installed and functional
if is_java_functional ; then
    echo "Java is already installed and functional."
else
    echo "Java is not installed or not functional. Installing openjdk@11..."

    # The 'brew' command must be available
    if ! type brew &> /dev/null ; then
        echo "Homebrew is not installed. Please install Homebrew first."
        exit 1
    fi

    # Install openjdk@11
    brew install openjdk@11

    # Ensure the installation was successful
    if [[ $? -ne 0 ]]; then
        echo "Failed to install openjdk@11"
        exit 1
    fi

    # Add JAVA_HOME environment variable and update PATH
    echo -e "\n# Set JAVA_HOME for OpenJDK 11" >> ~/.zshrc
    echo "export JAVA_HOME=$(/usr/libexec/java_home -v 11)" >> ~/.zshrc
    echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> ~/.zshrc

    # Reload shell configuration to apply changes
    source ~/.zshrc

    # Double-check if javac is available and functional now
    if ! is_java_functional ; then
        echo "Java installation failed. Please check the Homebrew logs."
        exit 1
    fi

    echo "Java installation completed successfully."
fi
