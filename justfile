install_vsce: 
    npm install -g vsce

flink-start:
    docker compose up -d

flink-stop:
    docker compose down

flink-start-producer:
    npm run produce:events -- ./flink-data/input

flink-run-job:
    docker compose exec jobmanager flink run -d \
    /opt/flink/examples/streaming/SocketWindowWordCount.jar \
    --hostname host.docker.internal --port 9999

build:
    vsce package
    mv *.vsix ./vsix/

publish:
    vsce publish

unzip_vsix:
    rm -rf ./unpacked_vsix
    mkdir -p ./unpacked_vsix
    latest=$(ls -t ./vsix/*.vsix 2>/dev/null | head -n1) && \
    if [ -z "$latest" ]; then echo "No .vsix files found in ./vsix"; exit 1; fi && \
    echo "Unzipping $latest into ./unpacked_vsix/" && \
    unzip "$latest" -d ./unpacked_vsix/

npm_outdated:
    npm outdated
    npx npm-check-updates

npm_update:
    npm update # update node_nodules and package-lock.json
    # these will not update packages in your package.json file

npm_reinstall:
    brew upgrade # upgrade homebrew
    brew install node # install the latest node version
    npm install -g npm@latest # upgrade to the latest version
    nvm alias default node # set the default node version
    nvm install node # install the latest node version

npm_doctor:
    node -v
    npm -v
    tsc -v
    npm doctor
    npm prune # remove unused dependencies
    npx depcheck # check dependencies
    npm-check # check dependencies
    
npm-install:
    rm -rf node_modules package-lock.json
    npm install
    npx tsc --noEmit

npm_rebuild:
    rm -rf node_modules
    npm install
