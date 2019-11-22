package sps.mrl.fee.calculation.onsite.ground;

/**
 * Zonal5_8Calculator
 *
 * @author Chilseasai@
 */
public class Zonal5_8Calculator {
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
        return 3.14;
    }

    private double calculateLargeStandardSize(final double shippingWeight) {
        if (shippingWeight <= 1.0) {
            return 5.43;
        } else if (shippingWeight > 1.0 && shippingWeight <= 2.0) {
            return 6.23;
        } else { // shippingWeight > 2.0
            return 6.23 + 0.35 * (shippingWeight - 2.0);
        }
    }

    private double calculateSmallOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 6.23;
        } else {
            return 6.23 + 0.35 * (shippingWeight - 2.0);
        }
    }

    private double calculateMediumOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 11.65;
        } else {
            return 11.65 + 0.38 * (shippingWeight - 2.0);
        }
    }

    private double calculateLargeOversize(final double shippingWeight) {
        if (shippingWeight <= 90.0) {
            return 33.67;
        } else {
            return 33.67 + 0.41 * (shippingWeight - 90.0);
        }
    }
}
