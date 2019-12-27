package main

import (
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"strconv"
)

func resourceApiKey() *schema.Resource {
	return &schema.Resource{
		Create: resourceApiKeyCreate,
		Read:   resourceApiKeyRead,
		//Update: resourceApiKeyUpdate,
		Delete: resourceApiKeyDelete,

		Schema: map[string]*schema.Schema{
			"account_id": &schema.Schema{
				Type:     schema.TypeString,
				ForceNew: true,
				Computed: true,
				Optional: true,
			},
			"cluster_id": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"key": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
			"secret": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
			"created": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
			"modified": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
		},
	}
}

func resourceApiKeyCreate(d *schema.ResourceData, m interface{}) error {
	config := m.(*Config)
	if err := config.connect(); err != nil {
		return err
	}

	accountId, accountIdSet := d.GetOk("account_id")
	if ! accountIdSet {
		accountId = config.Me.Account.Id
	}
	clusterId := d.Get("cluster_id").(string)

	cluster, err := config.getClusterPerAccount(accountId.(string), clusterId)
	if err != nil {
		return err
	}

	apiKey, err := config.createApiKey(*cluster)
	if err != nil {
		return err
	}
	d.SetId(strconv.Itoa(apiKey.Id))
	d.Set("secret", apiKey.Secret)
	return resourceApiKeyRead(d, m)
}

func resourceApiKeyRead(d *schema.ResourceData, m interface{}) error {
	config := m.(*Config)
	if err := config.connect(); err != nil {
		return err
	}

	accountId, accountIdSet := d.GetOk("account_id")
	if ! accountIdSet {
		accountId = config.Me.Account.Id
	}
	keyId := d.Id()
	keyIdInt, _ := strconv.Atoi(keyId)
	clusterId := d.Get("cluster_id").(string)

	cluster, err := config.getClusterPerAccount(accountId.(string), clusterId)
	if err != nil {
		return err
	}
	apiKey, err := config.getApiKey(*cluster, keyIdInt)
	if err != nil {
		return err
	}
	if apiKey == nil {
		d.SetId("")
		return nil
	}

	d.Set("key", apiKey.Key)
	//d.Set("secret", apiKey.Secret) //FIXME not sure
	d.Set("created", apiKey.Created)
	d.Set("modified", apiKey.Modified)

	return nil
}

func resourceApiKeyDelete(d *schema.ResourceData, m interface{}) error {
	config := m.(*Config)
	if err := config.connect(); err != nil {
		return err
	}

	accountId, accountIdSet := d.GetOk("account_id")
	if ! accountIdSet {
		accountId = config.Me.Account.Id
	}
	keyId := d.Id()
	keyIdInt, _ := strconv.Atoi(keyId)
	clusterId := d.Get("cluster_id").(string)
	cluster, err := config.getClusterPerAccount(accountId.(string), clusterId)
	if err != nil {
		return err
	}
	errDeleteApiKey := config.deleteApiKey(*cluster, keyIdInt)
	if errDeleteApiKey != nil {
		return errDeleteApiKey
	}
	d.SetId("")
	return nil
}
