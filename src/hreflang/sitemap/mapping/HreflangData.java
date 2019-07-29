/**
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */
package hreflang.sitemap.mapping;

import lombok.Data;

import java.io.Serializable;

/**
 * HreflangData
 *
 * @author cn-seo-dev@
 */
@Data
public class HreflangData implements Serializable {

    private String loc;

    private AlternateData[] link;
}
