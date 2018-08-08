#!/bin/bash -x


BUILD_IMG="thunderdb/build"
TEST_WD=$(cd $(dirname $0)/; pwd)
PROJECT_DIR=$(cd ${TEST_WD}/../../; pwd)
BENCH_CONTAIN="bench10.250.1.8"

echo ${PROJECT_DIR}

cd ${PROJECT_DIR} && ./build.sh

if [ -d ${TEST_WD}/GNTE/scripts/bin ];then
    mv ${TEST_WD}/GNTE/scripts/bin{,.bak}
fi

cd ${PROJECT_DIR} && cp ./cleanupDB.sh ${TEST_WD}/GNTE/scripts
cd ${TEST_WD} && bash ./GNTE/scripts/cleanupDB.sh
cd ${PROJECT_DIR} && cp -r ./bin ${TEST_WD}/GNTE/scripts

cd ${TEST_WD} && cp -r ./conf/* ./GNTE/scripts

cd ${TEST_WD}/GNTE && bash -x ./build.sh
cd ${TEST_WD}/GNTE && bash -x ./generate.sh ./scripts/gnte.yaml
rm -rf ${TEST_WD}/GNTE/scripts/bin.bak

INSIDE_GOPATH=$(docker run -it --rm ${BUILD_IMG} bash -c 'echo -n "$GOPATH"')
docker run -itd \
    --name ${BENCH_CONTAIN}\
    --net container:client10.250.1.8 \
    -v ${PROJECT_DIR}/../:${INSIDE_GOPATH}/src/gitlab.com/thunderdb/ \
    ${BUILD_IMG} tail -f /dev/null

docker exec -it ${BENCH_CONTAIN} bash -c "cd ${INSIDE_GOPATH}/src/gitlab.com/thunderdb/ThunderDB/client && go test -bench . -run BenchmarkThunderDBDriver"

docker rm -f ${BENCH_CONTAIN}

