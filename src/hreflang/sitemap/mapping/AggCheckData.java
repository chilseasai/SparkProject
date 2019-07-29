/**
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */
package hreflang.sitemap.mapping;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * AggCheckData
 *
 * @author cn-seo-dev@
 */
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AggCheckData implements Serializable {

    private String id;

    private String loc;

    private int numOfAlter;
}
