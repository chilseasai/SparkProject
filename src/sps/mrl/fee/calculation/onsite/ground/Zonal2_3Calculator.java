package sps.mrl.fee.calculation.onsite.ground;

/**
 * Zonal2_3Calculator
 *
 * @author Chilseasai@
 */
public class Zonal2_3Calculator {

    public double getAmount(final TestCaseItem item) {
        switch (item.getSizeTier()) {
            case "UsSmallStandardSize":
                return calculateSmallStandardSize(item.getShippingWeight());
            case "UsLargeStandardSize":
                return calculateLargeStandardSize(item.getShippingWeight());
            case "UsSmallOversize":
                return calculateSmallOversize(item.getShippingWeight());
            case "UsMediumOversize":
                return calculateMediumOversize(item.getShippingWeight());
            case "UsLargeOversize":
                return calculateLargeOversize(item.getShippingWeight());
            default:
                return 0.0;
        }
    }

    private double calculateSmallStandardSize(final double shippingWeight) {
        return 2.52;
    }

    private double calculateLargeStandardSize(final double shippingWeight) {
        if (shippingWeight <= 1.0) {
            return 3.47;
        } else if (shippingWeight > 1.0 && shippingWeight <= 2.0) {
            return 3.86;
        } else { // shippingWeight > 2.0
            return 3.86 + 0.13 * (shippingWeight - 2.0);
        }
    }

    private double calculateSmallOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 3.86;
        } else {
            return 3.86 + 0.13 * (shippingWeight - 2.0);
        }
    }

    private double calculateMediumOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 9.28;
        } else {
            return 9.28 + 0.25 * (shippingWeight - 2.0);
        }
    }

    private double calculateLargeOversize(final double shippingWeight) {
        if (shippingWeight <= 90.0) {
            return 31.30;
        } else {
            return 31.30 + 0.35 * (shippingWeight - 90.0);
        }
    }
}
