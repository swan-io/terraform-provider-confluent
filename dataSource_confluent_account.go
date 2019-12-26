package main

import (
	"errors"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"log"
)

func dataSourceConfluentAccount() *schema.Resource {
	return &schema.Resource{
		Read: dataSourceAccountRead,

		Schema: map[string]*schema.Schema{
			"name": &schema.Schema{
				Type:     schema.TypeString,
				ForceNew: true,
				Optional: true,
			},
			"deactivated": &schema.Schema{
				Type:     schema.TypeBool,
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
			"internal": &schema.Schema{
				Type:     schema.TypeBool,
				Computed: true,
			},
		},
	}
}

func dataSourceAccountRead(d *schema.ResourceData, m interface{}) error {
	config := m.(*Config)
	if err := config.connect(); err != nil {
		return err
	}
	accountName, accountNameOk := d.GetOk("name")

	if ! accountNameOk {
		log.Printf("Reading default account: " + config.Me.Account.Name)
		d.SetId(config.Me.Account.Id)
		d.Set("name", config.Me.Account.Name)
		d.Set("deactivated", config.Me.Account.Deactivated)
		d.Set("created", config.Me.Account.Created)
		d.Set("modified", config.Me.Account.Modified)
		d.Set("internal", config.Me.Account.Internal)
		return nil
	}
	log.Printf("Reading account: " + accountName.(string))
	for _, account := range config.Me.Accounts {
		if account.Name == accountName.(string) {
			d.SetId(account.Id)
			d.Set("name", account.Name)
			d.Set("deactivated", account.Deactivated)
			d.Set("created", account.Created)
			d.Set("modified", account.Modified)
			d.Set("internal", account.Internal)
			return nil
		}
	}
	return errors.New("Unable to find account with name " + d.Get("name").(string))
}
