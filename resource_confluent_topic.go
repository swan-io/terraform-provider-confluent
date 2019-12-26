package main

import (
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"log"
	"strconv"
	"strings"
)

func resourceTopic() *schema.Resource {
	return &schema.Resource{
		Create: resourceTopicCreate,
		Read:   resourceTopicRead,
		Update: resourceTopicUpdate,
		Delete: resourceTopicDelete,

		Schema: map[string]*schema.Schema{
			"account_id": &schema.Schema{
				Type:     schema.TypeString,
				ForceNew: true,
				Optional: true,
			},
			"cluster_id": &schema.Schema{
				Type:     schema.TypeString,
				ForceNew: true,
				Required: true,
			},
			"cluster_name": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
			"name": &schema.Schema{
				Type:     schema.TypeString,
				ForceNew: true,
				Required: true,
			},
			"num_partitions": &schema.Schema{
				Type:     schema.TypeInt,
				Optional: true,
				ForceNew: true,
				Default:  3,
			},
			"cleanup_policy": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				Default:  "delete",
			},
			"retention_ms": &schema.Schema{
				Type:     schema.TypeInt,
				Optional: true,
				Default:  604800000,
			},
			"segment_bytes": &schema.Schema{
				Type:     schema.TypeInt,
				Optional: true,
				Default:  1073741824,
			},
			"max_message_bytes": &schema.Schema{
				Type:     schema.TypeInt,
				Optional: true,
				Default:  2097164,
			},
			"min_compaction_lag_ms": &schema.Schema{
				Type:     schema.TypeInt,
				Optional: true,
				Default:  0,
			},
			"message_timestamp_type": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				Default:  "CreateTime",
			},
			"delete_retention_ms": &schema.Schema{
				Type:     schema.TypeInt,
				Optional: true,
				Default:  86400000,
			},
			"retention_bytes": &schema.Schema{
				Type:     schema.TypeInt,
				Optional: true,
				Default:  -1,
			},
			"segment_ms": &schema.Schema{
				Type:     schema.TypeInt,
				Optional: true,
				Default:  604800000,
			},
			"message_timestamp_difference_max_ms": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				Default:  "9223372036854775807",
			},
		},
	}
}

func getParams(d *schema.ResourceData) []KafkaTopicConfig {
	params := []KafkaTopicConfig{
		{
			Name:  "cleanup.policy",
			Value: d.Get("cleanup_policy").(string),
		},
		{
			Name:  "retention.ms",
			Value: strconv.Itoa(d.Get("retention_ms").(int)),
		},
		{
			Name:  "segment.bytes",
			Value: strconv.Itoa(d.Get("segment_bytes").(int)),
		},
		{
			Name:  "max.message.bytes",
			Value: strconv.Itoa(d.Get("max_message_bytes").(int)),
		},
		{
			Name:  "min.compaction.lag.ms",
			Value: strconv.Itoa(d.Get("min_compaction_lag_ms").(int)),
		},
		{
			Name:  "message.timestamp.type",
			Value: d.Get("message_timestamp_type").(string),
		},
		{
			Name:  "delete.retention.ms",
			Value: strconv.Itoa(d.Get("delete_retention_ms").(int)),
		},
		{
			Name:  "retention.bytes",
			Value: strconv.Itoa(d.Get("retention_bytes").(int)),
		},
		{
			Name:  "segment.ms",
			Value: strconv.Itoa(d.Get("segment_ms").(int)),
		},
		{
			Name:  "message.timestamp.difference.max.ms",
			Value: d.Get("message_timestamp_difference_max_ms").(string),
		},
	}
	return params
}

func resourceTopicCreate(d *schema.ResourceData, m interface{}) error {
	config := m.(*Config)
	if err := config.connect(); err != nil {
		return err
	}

	name := d.Get("name").(string)
	accountId, accountIdSet := d.GetOk("account_id")
	if ! accountIdSet {
		accountId = config.Me.Account.Id
	}
	clusterId := d.Get("cluster_id").(string)

	log.Printf("Creating topic " + name + " in account " + accountId.(string) + " for cluster " + clusterId)

	params := getParams(d)

	cluster, err := config.getClusterPerAccount(accountId.(string), clusterId)
	if err != nil {
		return err
	}

	err = config.createTopic(*cluster, name, d.Get("num_partitions").(int), params)
	if err != nil {
		return err
	}

	d.SetId(accountId.(string) + "-" + clusterId + "-" + name)
	return resourceTopicRead(d, m)
}

func resourceTopicRead(d *schema.ResourceData, m interface{}) error {
	config := m.(*Config)
	if err := config.connect(); err != nil {
		return err
	}

	accountId, accountIdSet := d.GetOk("account_id")
	if ! accountIdSet {
		accountId = config.Me.Account.Id
	}
	clusterId := d.Get("cluster_id")

	log.Printf("Reading topic: " + d.Get("name").(string) + " for account " + accountId.(string) + " and cluster " + clusterId.(string))

	cluster, err := config.getClusterPerAccount(accountId.(string), clusterId.(string))
	if err != nil {
		d.SetId("")
		return nil
	}
	topic, err := config.getTopic(*cluster, d.Get("name").(string))
	if err != nil {
		d.SetId("")
		return nil
	}
	for _, topicConfig := range topic.Configs {
		if !topicConfig.ReadOnly {
			//log.Printf(strings.ReplaceAll(topicConfig.Name, ".", "_")+"="+topicConfig.Value)
			d.Set(strings.ReplaceAll(topicConfig.Name, ".", "_"), topicConfig.Value)
		}
	}

	d.Set("name", topic.Name)
	d.Set("cluster_id", cluster.Id)
	d.Set("cluster_name", cluster.Name)
	d.Set("num_partitions", len(topic.Partitions))

	return nil
}

func resourceTopicUpdate(d *schema.ResourceData, m interface{}) error {
	config := m.(*Config)
	if err := config.connect(); err != nil {
		return err
	}

	accountId, accountIdSet := d.GetOk("account_id")
	if ! accountIdSet {
		accountId = config.Me.Account.Id
	}
	clusterId := d.Get("cluster_id").(string)
	name := d.Get("name").(string)

	log.Printf("Updating topic " + name + " in account " + accountId.(string) + " for cluster " + clusterId)

	params := getParams(d)

	cluster, err := config.getClusterPerAccount(accountId.(string), clusterId)
	if err != nil {
		return nil
	}

	err = config.updateTopicConfig(*cluster, name, params)
	if err != nil {
		return nil
	}
	return resourceTopicRead(d, m)
}

func resourceTopicDelete(d *schema.ResourceData, m interface{}) error {
	log.Printf("Deleting topic: " + d.Get("name").(string))
	config := m.(*Config)
	if err := config.connect(); err != nil {
		return err
	}

	accountId, accountIdSet := d.GetOk("account_id")
	if ! accountIdSet {
		accountId = config.Me.Account.Id
	}
	clusterId := d.Get("cluster_id").(string)
	name := d.Get("name").(string)
	log.Printf("Deleting topic " + name + " in account " + accountId.(string) + " for cluster " + clusterId)

	cluster, err := config.getClusterPerAccount(accountId.(string), clusterId)
	if err != nil {
		return err
	}
	config.deleteTopic(*cluster, name)

	d.SetId("")
	return nil
}
