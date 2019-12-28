package main

import (
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"log"
)

func dataSourceConfluentCluster() *schema.Resource {
	return &schema.Resource{
		Read: dataSourceClusterRead,

		Schema: map[string]*schema.Schema{
			"name": &schema.Schema{
				Type:        schema.TypeString,
				ForceNew:    true,
				Required:    true,
				Description: "Cluster name",
			},
			"account_id": &schema.Schema{
				Type:        schema.TypeString,
				ForceNew:    true,
				Optional:    true,
				Description: "Account ID. If not set, using default account",
			},
			"organization_id": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
			"endpoint": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
			"api_endpoint": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
			"status": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
			"durability": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
			"host": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
			"port": &schema.Schema{
				Type:     schema.TypeInt,
				Computed: true,
			},
			"protocol": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
		},
	}
}

func dataSourceClusterRead(d *schema.ResourceData, m interface{}) error {
	config := m.(*Config)
	if err := config.connect(); err != nil {
		return err
	}
	accountId, accountIdSet := d.GetOk("account_id")
	if !accountIdSet {
		accountId = config.Me.Account.Id
	}
	log.Printf("Reading cluster: " + d.Get("name").(string) + " and account " + accountId.(string))
	cluster, err := config.getCluster(accountId.(string), d.Get("name").(string))
	if err != nil {
		d.SetId("")
		return nil
	}

	d.Set("name", cluster.Name)
	d.Set("account_id", accountId.(string))
	d.Set("endpoint", cluster.Endpoint)
	d.Set("api_endpoint", cluster.ApiEndpoint)
	d.Set("status", cluster.Status)
	d.Set("durability", cluster.Durability)
	d.Set("host", cluster.Host())
	d.Set("port", cluster.Port())
	d.Set("protocol", cluster.Protocol())
	d.SetId(cluster.Id)
	return nil
}
