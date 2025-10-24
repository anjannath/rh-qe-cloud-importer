package aws

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/ec2"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/redhat-developer/mapt/pkg/provider/aws/data"
)

type replicateRequest struct {
	amiName       string
	amiRegion     *string
	amiId         *string
	targetRegions []string
}

type amiName int

const (
	openshiftLocal amiName = iota
	rhelai
)

var amiOwners = map[amiName]string{
	openshiftLocal: "391597328979",
	rhelai: "610952687893",
}

func getAmiOwner(amiName string) string {
	if strings.Contains(amiName, "openshift") {
		return amiOwners[openshiftLocal]
	}
	if strings.Contains(amiName, "rhelai") {
		return amiOwners[rhelai]
	}
	return ""
}

func (a *aws) Replicate(amiName, amiArch string, targetRegions []string) (pulumi.RunFunc, []string, error) {
	var availableRegions []string
	var err error

	regions, err := data.GetRegions()
	if err != nil {
		return nil, []string{}, err
	}

	if slices.Contains(targetRegions, "all") {
		availableRegions = regions
	} else {
		availableRegions = targetRegions
	}

	r := make(chan *data.ImageInfo, len(regions))
	e := make(chan string, 1)
	closeCh := make(chan string, 1)

	defer close(r)
	defer close(closeCh)

	var wg sync.WaitGroup
	for _, region := range regions {
		wg.Add(1)
		lRegion := region
		amiOwner := getAmiOwner(amiName)
		go func(r chan *data.ImageInfo, closeCh chan string) {
			defer wg.Done()
			select {
			case <-closeCh:
				return
			default:
				if isOffered, i, _ := data.IsAMIOffered(
					data.ImageRequest{
						Name:   &amiName,
						Arch:   &amiArch,
						Owner:  &amiOwner,
						Region: &lRegion,
					}); isOffered {
					r <- i
				}
			}
		}(r, closeCh)
	}
	go func(e chan string) {
		wg.Wait()
		defer close(e)
		e <- "done"
	}(e)
	select {
	case sAMI := <-r:
		closeCh <- "done"
		r := replicateRequest{
			amiName,
			sAMI.Region,
			sAMI.Image.ImageId,
			availableRegions,
		}
		return r.runFunc, availableRegions, nil
	case <-e:
		return nil, []string{}, fmt.Errorf("did not find any AMI with name %s in any region", amiName)
	}

}

func (r replicateRequest) runFunc(ctx *pulumi.Context) error {
	return replicateAMI(ctx, r.amiName, r.amiId, r.amiRegion)
}

func replicateAMI(ctx *pulumi.Context, amiName string, srcAmiId, srcAmiRegion *string) error {
	region, _ := ctx.GetConfig("aws:region")
	if region == *srcAmiRegion {
		return nil
	}
	_, err := ec2.NewAmiCopy(ctx,
		amiName,
		&ec2.AmiCopyArgs{
			Description: pulumi.String(
				fmt.Sprintf("Replica of %s from %s", *srcAmiId, *srcAmiRegion)),
			SourceAmiId:     pulumi.String(*srcAmiId),
			SourceAmiRegion: pulumi.String(*srcAmiRegion),
		},
		pulumi.RetainOnDelete(true))
	if err != nil {
		return err
	}

	return nil
}
