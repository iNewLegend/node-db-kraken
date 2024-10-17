#!/bin/bash
REQUIRED_JAVA_VERSION=17
PROFILE_FILE="$HOME/.bashrc"  # Change this to .bash_profile or .profile if needed

# Function to get installed Java version
get_java_version() {
  java -version 2>&1 | awk -F '"' '/version/ {print $2}'
}

# Function to parse version string and convert it to an integer for comparison
version_to_int() {
  local version=$1
  printf '%d%02d%02d' $(echo "$version" | awk -F. '{print $1, $2, $3}')
}

# Function to compare versions
version_ge() {
  local version1=$(version_to_int "$1")
  local version2=$(version_to_int "$2")
  [ "$version1" -ge "$version2" ]
}

# Function to save JAVA_HOME and update PATH in the shell profile
save_java_home_to_profile() {
  local java_home=$1
  local profile_file="$PROFILE_FILE"

  # Create the profile file if it doesn't exist
  touch "$profile_file"

  # Check if JAVA_HOME is already set in the profile file
  if grep -q "export JAVA_HOME=" "$profile_file"; then
    # Update existing JAVA_HOME entry
    sed -i.bak "/^export JAVA_HOME=/c\export JAVA_HOME=$java_home" "$profile_file"
  else
    # Add new JAVA_HOME entry
    echo "export JAVA_HOME=$java_home" >> "$profile_file"
  fi

  # Check if PATH modification is already set in the profile file
  if grep -q "PATH=.*$JAVA_HOME/bin.*" "$profile_file"; then
    # Update existing PATH entry
    sed -i.bak "/PATH=.*$JAVA_HOME/bin.*/c\export PATH=\"$java_home/bin:\$PATH\"" "$profile_file"
  else
    # Add new PATH entry
    echo "export PATH=\"$java_home/bin:\$PATH\"" >> "$profile_file"
  fi
}

# Function to reload the profile file
reload_profile() {
  source "$PROFILE_FILE"
  echo "The environment has been reloaded."
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
    echo "javac not found or not functional."
    return 1
  fi

  java_version=$(get_java_version)
  echo "Current Java version: $java_version"

  if [ -z "$java_version" ]; then
    echo "Java is not installed. Attempting to install Java $REQUIRED_JAVA_VERSION..."
    return 1
  fi

  if version_ge "$java_version" "$REQUIRED_JAVA_VERSION"; then
    echo "Java $REQUIRED_JAVA_VERSION or newer is already installed."
    return 0
  else
    echo "Java version is less than $REQUIRED_JAVA_VERSION. Attempting to install Java $REQUIRED_JAVA_VERSION..."
    return 1
  fi
}

# Function to install Java using the available package manager
install_java() {
  if type apt > /dev/null 2>&1; then
    sudo apt update
    sudo apt install -y openjdk-${REQUIRED_JAVA_VERSION}-jdk
  elif type yum > /dev/null 2>&1; then
    sudo yum install -y java-${REQUIRED_JAVA_VERSION}-openjdk-devel
  else
    echo "Neither apt nor yum is available. Please install one of these package managers."
    exit 1
  fi

  local java_home
  java_home=$(update-alternatives --list java | grep "java-${REQUIRED_JAVA_VERSION}-openjdk-amd64")
  if [ -n "$java_home" ]; then
    save_java_home_to_profile "$java_home"
    reload_profile
  else
    echo "Failed to find the newly installed Java home."
    exit 1
  fi
}

# Select the desired Java version
select_java_version() {
  echo "Selecting Java $REQUIRED_JAVA_VERSION..."
  sudo update-alternatives --set java /usr/lib/jvm/java-17-openjdk-amd64/bin/java

  local java_home
  java_home="/usr/lib/jvm/java-17-openjdk-amd64"
  save_java_home_to_profile "$java_home"
  reload_profile

  return 0
}

# Main script logic
if is_java_functional; then
  echo "Java $REQUIRED_JAVA_VERSION or higher is already installed and functional."
else
  echo "Installing Java $REQUIRED_JAVA_VERSION..."
  install_java

  # Ensure the installation was successful
  hash -r # Rehash to recognize the newly installed Java immediately
  if is_java_functional; then
    echo "Java $REQUIRED_JAVA_VERSION or higher has been installed and configured successfully."
  else
    echo "Failed to install Java $REQUIRED_JAVA_VERSION. Trying to select manually..."
    select_java_version
  fi
fi
