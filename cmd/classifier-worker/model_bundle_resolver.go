package main

import (
	"context"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

type servingBackedModelBundleResolver struct {
	serving application.ServingBundleResolver
}

var _ application.ModelBundleResolver = (*servingBackedModelBundleResolver)(nil)

func (resolver *servingBackedModelBundleResolver) Resolve(ctx context.Context, modelBundleVersion string) (application.ModelBundleMetadata, error) {
	servingBundle, err := resolver.serving.ResolveServingBundle(ctx, modelBundleVersion)
	if err != nil {
		return application.ModelBundleMetadata{}, err
	}

	return application.ModelBundleMetadata{
		ModelBundleVersion: servingBundle.ModelBundleVersion,
	}, nil
}
