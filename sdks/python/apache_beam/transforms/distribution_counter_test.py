# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

'''
Unit tests for the Distribution Counter
'''
import math
import unittest

from mock import Mock
from nose.plugins.skip import SkipTest

class DistributionAccumulatorTest(unittest.TestCase):

  def setUp(self):
    try:
      #pylint: disable=global-variable-not-assigned
      global DistributionAccumulator
      from apache_beam.transforms.distribution_counter \
        import DistributionAccumulator
    except ImportError:
      raise SkipTest('DistributionAccumulator not complied.')

  def test_calculate_bucket_index_with_input_0(self):
    counter = DistributionAccumulator()
    index = counter.calculate_bucket_index_for_test(0)
    self.assertEquals(index, 0)

  def test_calculate_bucket_index_within_max_long(self):
    counter = DistributionAccumulator()
    bucket = 1
    power_of_ten = 1
    INT64_MAX = math.pow(2, 63) - 1
    while power_of_ten <= INT64_MAX:
      for multiplier in [1, 2, 5]:
        value = multiplier * power_of_ten
        actual_bucket = counter.calculate_bucket_index_for_test(value - 1)
        self.assertEquals(actual_bucket, bucket - 1)
        bucket += 1
      power_of_ten *= 10

  def test_add_input(self):
    counter = DistributionAccumulator()
    expected_buckets = [1, 3, 0, 0, 0, 0, 0, 0, 1, 1]
    expected_sum = 1510
    expected_first_bucket_index = 1
    expected_count = 6
    expected_min = 1
    expected_max = 1000
    counter.add_inputs_for_test([1, 500, 2, 3, 1000, 4])
    histogram = Mock(firstBucketOffset=None, bucketCounts=None)
    counter.translate_to_histogram(histogram)
    self.assertEquals(counter.sum, expected_sum)
    self.assertEquals(counter.count, expected_count)
    self.assertEquals(counter.min, expected_min)
    self.assertEquals(counter.max, expected_max)
    self.assertEquals(histogram.firstBucketOffset, expected_first_bucket_index)
    self.assertEquals(histogram.bucketCounts, expected_buckets)


if __name__ == '__main__':
  unittest.main()
