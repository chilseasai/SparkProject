/**
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */
package hreflang.sitemap.mapping;

import lombok.Data;

import java.io.Serializable;

/**
 * AlternateData
 *
 * @author cn-seo-dev@
 */
@Data
public class AlternateData implements Serializable {

    private String href;

    private String hreflang;

    private String rel;
}
