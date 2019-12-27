package main

import (
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

func Provider() *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"email": {
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("CONFLUENT_EMAIL", nil),
				Description: "Confluent email",
			},
			"password": {
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("CONFLUENT_PASSWORD", nil),
				Description: "Confluent password",
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"confluent_cluster": resourceCluster(),
			"confluent_topic":   resourceTopic(),
		},
		DataSourcesMap: map[string]*schema.Resource{
			"confluent_cluster": dataSourceConfluentCluster(),
			"confluent_account": dataSourceConfluentAccount(),
		},
		ConfigureFunc: configureProvider,
	}
}

func configureProvider(d *schema.ResourceData) (interface{}, error) {
	config := Config{
		ApiEndpoint: "https://confluent.cloud",
		Email:       d.Get("email").(string),
		Password:    d.Get("password").(string),
	}

	return &config, nil
}
