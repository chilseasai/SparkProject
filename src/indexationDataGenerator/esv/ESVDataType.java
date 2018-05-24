package indexationDataGenerator.esv;

import lombok.Data;
import lombok.Setter;

import java.io.Serializable;

/**
 * ESVDataType
 *
 * @author cn-seo-dev@
 */
@Data
public class ESVDataType implements Serializable {

    private Integer marketplace_id;

    private String page_id;

    private Integer number_of_refinement;

    private String keyword;

    private Integer check_esv;

    private String create_date;

    @Setter
    private Integer esv;
}
