package kafka.hw3.streams;

import com.google.common.net.InternetDomainName;
import io.micrometer.common.util.StringUtils;

import java.net.URI;
import java.util.Optional;

public final class DomainExtractor {

    private DomainExtractor() {
    }

    public static Optional<String> extractTopLevelDomain(String rawUrl) {
        if (StringUtils.isBlank(rawUrl)) {
            return Optional.empty();
        }

        try {
            String host = URI.create(rawUrl.trim()).getHost();
            if (StringUtils.isBlank(host)) {
                return Optional.empty();
            }

            InternetDomainName domainName = InternetDomainName.from(host);
            if (domainName.parts().isEmpty()) {
                return Optional.empty();
            }

            // TLD - uk\au instead of co.uk com.au
            return Optional.of(domainName.parts().get(domainName.parts().size() - 1));
        } catch (IllegalArgumentException | NullPointerException e) {
            return Optional.empty();
        }
    }
}
