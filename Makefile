TAG    := latest
PROJ   := stock-tool
PWD    := $(dir $(realpath $(lastword $(MAKEFILE_LIST))))

default: image


image:
	docker build -t $(PROJ):$(TAG) .

get-data:
	docker run --rm\
		-v $(PWD)/extraData:/app/extraData \
		-w /app \
		$(PROJ):$(TAG) \
		python get_extraData.py

report:
	docker run --rm\
		-v $(PWD)/extraData:/app/extraData \
		-v $(PWD)/report:/app/report \
		-w /app \
		$(PROJ):$(TAG) \
		python stock.py

.PHONY: image report get-data default
