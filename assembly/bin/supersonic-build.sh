#!/usr/bin/env bash

sbinDir=$(cd "$(dirname "$0")"; pwd)
chmod +x $sbinDir/supersonic-common.sh
source $sbinDir/supersonic-common.sh
cd $projectDir

MVN_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout | grep -v '^\[' | sed -n '/^[0-9]/p')
if [ -z "$MVN_VERSION" ]; then
  echo "Failed to retrieve Maven project version."
  exit 1
fi
echo "Maven project version: $MVN_VERSION"

cd $baseDir
service=$1
if [ -z "$service"  ]; then
  service=${STANDALONE_SERVICE}
fi

function buildJavaService {
  model_name=$1
  echo "starting building supersonic-${model_name} service"
  mvn -f $projectDir clean package -DskipTests -Dspotless.skip=true
  if [ $? -ne 0 ]; then
      echo "Failed to build backend Java modules."
      exit 1
  fi
  cp $projectDir/launchers/${model_name}/target/*.tar.gz ${buildDir}/
  echo "finished building supersonic-${model_name} service"
}

function buildWebapp {
  echo "starting building supersonic webapp"
  chmod +x $projectDir/webapp/start-fe-prod.sh
  cd $projectDir/webapp
  sh ./start-fe-prod.sh
  # check build result
  if [ $? -ne 0 ]; then
      echo "Failed to build frontend webapp."
      exit 1
  fi
  cp -fr  ./supersonic-webapp.tar.gz ${buildDir}/
  # check build result
  if [ $? -ne 0 ]; then
      echo "Failed to get supersonic webapp package."
      exit 1
  fi
  echo "finished building supersonic webapp"
}

function syncWebappToStandaloneClasses {
  target_path=$projectDir/launchers/$STANDALONE_SERVICE/target/classes
  archive_path=$projectDir/webapp/supersonic-webapp.tar.gz
  echo "syncing supersonic webapp to standalone classes: $target_path"
  mkdir -p "$target_path"
  tar xvf "$archive_path" -C "$target_path"
  if [ $? -ne 0 ]; then
      echo "Failed to extract supersonic webapp package to $target_path."
      exit 1
  fi
  rm -rf "$target_path/webapp"
  if [ ! -d "$target_path/supersonic-webapp" ]; then
      echo "Failed to find extracted supersonic webapp directory in $target_path."
      exit 1
  fi
  mv "$target_path/supersonic-webapp" "$target_path/webapp"
}

function packageRelease {
  model_name=$1
  release_dir=supersonic-${model_name}-${MVN_VERSION}
  service_name=launchers-${model_name}-${MVN_VERSION}
  webapp_archive=supersonic-webapp.tar.gz
  service_archive=${service_name}-bin.tar.gz
  echo "starting packaging supersonic release"
  cd "$buildDir"
  [ -d "$release_dir" ] && rm -rf "$release_dir"
  [ -f "$release_dir.zip" ] && rm -f "$release_dir.zip"
  mkdir -p "$release_dir"
  # package webapp
  if [ ! -f "$webapp_archive" ]; then
      echo "Failed to find $webapp_archive under $buildDir."
      exit 1
  fi
  tar xvf "$webapp_archive"
  if [ $? -ne 0 ] || [ ! -d "supersonic-webapp" ]; then
      echo "Failed to extract supersonic webapp package."
      exit 1
  fi
  mv supersonic-webapp webapp
  # check webapp build result
  if [ $? -ne 0 ]; then
      echo "Failed to get supersonic webapp package."
      exit 1
  fi
  json='{"env": "''"}'
  echo $json > webapp/supersonic.config.json
  mv webapp "$release_dir/"
  # package java service
  if [ ! -f "$service_archive" ]; then
      echo "Failed to find $service_archive under $buildDir."
      exit 1
  fi
  tar xvf "$service_archive"
  if [ $? -ne 0 ] || [ ! -d "$service_name" ]; then
      echo "Failed to extract $service_archive."
      exit 1
  fi
  mv "$service_name"/* "$release_dir/"
  if [ $? -ne 0 ]; then
      echo "Failed to move packaged service files into $release_dir."
      exit 1
  fi
  # generate zip file
  zip -r "$release_dir.zip" "$release_dir"
  if [ $? -ne 0 ]; then
      echo "Failed to generate $release_dir.zip."
      exit 1
  fi
  # delete intermediate files
  rm -f "$webapp_archive" "$service_archive"
  rm -rf webapp "$service_name" "$release_dir"
  echo "finished packaging supersonic release"
}

#1. build backend services
if [ "$service" == "webapp" ]; then
  buildWebapp
  syncWebappToStandaloneClasses
else
  buildJavaService $service
  buildWebapp
  packageRelease $service
fi
