package exceptionTest;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * R1Meta
 *
 * @author cn-seo-dev@
 */
@Data
@NoArgsConstructor
public class R1Meta implements Serializable {
    private Long marketplace_id;
    private String page_id;
    private String refinement_picker_value;
    private String refinement_short_id;
    private String refinement_name;
    private String browse_node_id;
    private String refinement_picker_node_id;
}
