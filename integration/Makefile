
all: s3test webdav ioctl

s3test:
	command -v aws >/dev/null 2>&1 || PIP_BREAK_SYSTEM_PACKAGES=1 python3 -m pip install awscli
	bash s3gateway_test.sh

webdav:
	@if [ ! -x /home/travis/.m2/litmus-0.13/basic ]; then \
		mkdir -p /home/travis/.m2/ ;\
		wget -O /home/travis/.m2/litmus-0.13.tar.gz http://www.webdav.org/neon/litmus/litmus-0.13.tar.gz ;\
		tar -zxvf /home/travis/.m2/litmus-0.13.tar.gz -C /home/travis/.m2/ ;\
		cd /home/travis/.m2/litmus-0.13/ && ./configure && make ;\
	fi
	cd /home/travis/.m2/litmus-0.13 ; for i in "basic" "copymove" "http"; do sudo ./$${i} http://127.0.0.1:9009 root 1234; done

ioctl:
	bash ioctl_test.sh /tmp/jfs-unit-test/ioctl_test 2>/dev/null
