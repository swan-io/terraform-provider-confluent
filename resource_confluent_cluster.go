package main

import (
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

func resourceCluster() *schema.Resource {
	return &schema.Resource{
		Create: resourceClusterCreate,
		Read:   resourceClusterRead,
		Update: resourceClusterUpdate,
		Delete: resourceClusterDelete,

		Schema: map[string]*schema.Schema{
			"account_id": &schema.Schema{
				Type:     schema.TypeString,
				ForceNew: true,
				Computed: true,
				Optional: true,
			},
			"name": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"service_provider": &schema.Schema{
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Cloud provider: aws, gcp or azure",
			},
			"region": &schema.Schema{
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Cloud provider region (eg: eu-west-1, us-central-1, westeurope, etc ...)",
			},
			"durability": &schema.Schema{
				Type:        schema.TypeString,
				ForceNew:    true,
				Optional:    true,
				Default:     "LOW",
				Description: "LOW or HIGH (multi-zone or Single zone)",
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
		},
	}
}

func resourceClusterCreate(d *schema.ResourceData, m interface{}) error {
	config := m.(*Config)
	if err := config.connect(); err != nil {
		return err
	}

	accountId, accountIdSet := d.GetOk("account_id")
	if ! accountIdSet {
		accountId = config.Me.Account.Id
	}
	clusterName := d.Get("name").(string)
	durability := d.Get("durability").(string)
	region := d.Get("region").(string)
	serviceProvider := d.Get("service_provider").(string)
	cluster, err := config.createClusterPerAccount(accountId.(string), clusterName, durability, region, serviceProvider)
	if err != nil {
		return err
	}

	d.SetId(cluster.Id)
	return resourceClusterRead(d, m)
}

func resourceClusterRead(d *schema.ResourceData, m interface{}) error {
	config := m.(*Config)
	if err := config.connect(); err != nil {
		return err
	}

	accountId, accountIdSet := d.GetOk("account_id")
	if ! accountIdSet {
		accountId = config.Me.Account.Id
	}
	clusterName := d.Get("name").(string)

	cluster, err := config.getClusterPerAccountAndName(accountId.(string), clusterName)
	if err != nil {
		return err
	}

	d.SetId(cluster.Id)
	d.Set("account_id", accountId.(string))
	d.Set("name", cluster.Name)
	d.Set("provider", cluster.ServiceProvider)
	d.Set("region", cluster.Region)
	d.Set("durability", cluster.Durability)
	d.Set("organization_id", cluster.OrganizationId)
	d.Set("endpoint", cluster.Endpoint)
	d.Set("api_endpoint", cluster.ApiEndpoint)
	d.Set("status", cluster.Status)

	return nil
}

func resourceClusterUpdate(d *schema.ResourceData, m interface{}) error {
	config := m.(*Config)
	if err := config.connect(); err != nil {
		return err
	}

	accountId, accountIdSet := d.GetOk("account_id")
	if ! accountIdSet {
		accountId = config.Me.Account.Id
	}
	oldClusterName, newClusterName := d.GetChange("name")

	cluster, err := config.getClusterPerAccountAndName(accountId.(string), oldClusterName.(string))
	if err != nil {
		return err
	}

	_, err = config.updateCluster(*cluster, newClusterName.(string))
	if err != nil {
		return nil
	}
	return resourceClusterRead(d, m)
}

func resourceClusterDelete(d *schema.ResourceData, m interface{}) error {
	config := m.(*Config)
	if err := config.connect(); err != nil {
		return err
	}

	accountId, accountIdSet := d.GetOk("account_id")
	if ! accountIdSet {
		accountId = config.Me.Account.Id
	}
	cluster, err := config.getClusterPerAccount(accountId.(string), d.Id())
	if err != nil {
		return err
	}
	errDeleteCluster := config.deleteCluster(*cluster)
	if errDeleteCluster != nil {
		return errDeleteCluster
	}
	d.SetId("")
	return nil
}
