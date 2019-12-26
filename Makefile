default: build

build:
		CGO_ENABLED=0 go build -o terraform-provider-confluent

debug: build
		cp terraform-provider-confluent ~/.terraform.d/plugins
		terraform init

clean:
		rm -f terraform-provider-confluent