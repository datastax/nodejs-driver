if (!String.prototype.trim) {
  (function() {
    // Make sure we trim BOM and NBSP
    var rtrim = /^[\s\uFEFF\xA0]+|[\s\uFEFF\xA0]+$/g;
    String.prototype.trim = function() {
        return this.replace(rtrim, '');
    };
  })();
}

(function(window) {
  function basePath() {
    var regexp = new RegExp('js/app.js');
    var script = $('script').filter(function(i, el) {
      return el.src.match(regexp);
    })[0]

    var base = script.src.substr(window.location.protocol.length + window.location.host.length + 2, script.src.length);

    return base.replace('/js/app.js', '');
  }

  var app = window.angular.module('docs', ['cfp.hotkeys'])

  app.value('pages', {"/":{"title":"Home","summary":"Home <small class=\"text-muted\">page</small>","path":"/","version":"v3.1"},"/features/address-resolution/":{"title":"Address resolution","summary":"Address resolution <small class=\"text-muted\">page</small>","path":"/features/address-resolution/","version":"v3.1"},"/features/batch/":{"title":"Batch statements","summary":"Batch statements <small class=\"text-muted\">page</small>","path":"/features/batch/","version":"v3.1"},"/features/connection-pooling/":{"title":"Connection pooling","summary":"Connection pooling <small class=\"text-muted\">page</small>","path":"/features/connection-pooling/","version":"v3.1"},"/features/datatypes/collections/":{"title":"Collections","summary":"Collections <small class=\"text-muted\">page</small>","path":"/features/datatypes/collections/","version":"v3.1"},"/features/datatypes/datetime/":{"title":"Date and time representation","summary":"Date and time representation <small class=\"text-muted\">page</small>","path":"/features/datatypes/datetime/","version":"v3.1"},"/features/datatypes/nulls/":{"title":"Null and unset values","summary":"Null and unset values <small class=\"text-muted\">page</small>","path":"/features/datatypes/nulls/","version":"v3.1"},"/features/datatypes/numerical/":{"title":"Numerical values","summary":"Numerical values <small class=\"text-muted\">page</small>","path":"/features/datatypes/numerical/","version":"v3.1"},"/features/datatypes/":{"title":"CQL data types to JavaScript types","summary":"CQL data types to JavaScript types <small class=\"text-muted\">page</small>","path":"/features/datatypes/","version":"v3.1"},"/features/datatypes/tuples/":{"title":"Tuples","summary":"Tuples <small class=\"text-muted\">page</small>","path":"/features/datatypes/tuples/","version":"v3.1"},"/features/datatypes/udts/":{"title":"User-defined types","summary":"User-defined types <small class=\"text-muted\">page</small>","path":"/features/datatypes/udts/","version":"v3.1"},"/features/datatypes/uuids/":{"title":"UUID and time-based UUID data types","summary":"UUID and time-based UUID data types <small class=\"text-muted\">page</small>","path":"/features/datatypes/uuids/","version":"v3.1"},"/features/execution-profiles/":{"title":"Execution Profiles (experimental)","summary":"Execution Profiles (experimental) <small class=\"text-muted\">page</small>","path":"/features/execution-profiles/","version":"v3.1"},"/features/metadata/":{"title":"Cluster and schema metadata","summary":"Cluster and schema metadata <small class=\"text-muted\">page</small>","path":"/features/metadata/","version":"v3.1"},"/features/native-protocol/":{"title":"Native protocol","summary":"Native protocol <small class=\"text-muted\">page</small>","path":"/features/native-protocol/","version":"v3.1"},"/features/paging/":{"title":"Fetching large result sets","summary":"Fetching large result sets <small class=\"text-muted\">page</small>","path":"/features/paging/","version":"v3.1"},"/features/parameterized-queries/":{"title":"Parameterized queries","summary":"Parameterized queries <small class=\"text-muted\">page</small>","path":"/features/parameterized-queries/","version":"v3.1"},"/features/query-warnings/":{"title":"Query warnings","summary":"Query warnings <small class=\"text-muted\">page</small>","path":"/features/query-warnings/","version":"v3.1"},"/features/":{"title":"Features","summary":"Features <small class=\"text-muted\">page</small>","path":"/features/","version":"v3.1"},"/features/tuning-policies/":{"title":"Tuning policies","summary":"Tuning policies <small class=\"text-muted\">page</small>","path":"/features/tuning-policies/","version":"v3.1"},"/features/udfs/":{"title":"User-defined functions and aggregates","summary":"User-defined functions and aggregates <small class=\"text-muted\">page</small>","path":"/features/udfs/","version":"v3.1"},"/faq/":{"title":"Frequently Asked Questions","summary":"Frequently Asked Questions <small class=\"text-muted\">page</small>","path":"/faq/","version":"v3.1"},"/getting-started/":{"title":"Getting started","summary":"Getting started <small class=\"text-muted\">page</small>","path":"/getting-started/","version":"v3.1"},"/coding-rules/":{"title":"Three simple rules for coding with the driver","summary":"Three simple rules for coding with the driver <small class=\"text-muted\">page</small>","path":"/coding-rules/","version":"v3.1"},"/v3.0/":{"title":"Home","summary":"Home <small class=\"text-muted\">page</small>","path":"/v3.0/","version":"v3.0"},"/v3.0/features/address-resolution/":{"title":"Address resolution","summary":"Address resolution <small class=\"text-muted\">page</small>","path":"/v3.0/features/address-resolution/","version":"v3.0"},"/v3.0/features/batch/":{"title":"Batch statements","summary":"Batch statements <small class=\"text-muted\">page</small>","path":"/v3.0/features/batch/","version":"v3.0"},"/v3.0/features/connection-pooling/":{"title":"Connection pooling","summary":"Connection pooling <small class=\"text-muted\">page</small>","path":"/v3.0/features/connection-pooling/","version":"v3.0"},"/v3.0/features/datatypes/collections/":{"title":"Collections","summary":"Collections <small class=\"text-muted\">page</small>","path":"/v3.0/features/datatypes/collections/","version":"v3.0"},"/v3.0/features/datatypes/datetime/":{"title":"Date and time representation","summary":"Date and time representation <small class=\"text-muted\">page</small>","path":"/v3.0/features/datatypes/datetime/","version":"v3.0"},"/v3.0/features/datatypes/nulls/":{"title":"Null and unset values","summary":"Null and unset values <small class=\"text-muted\">page</small>","path":"/v3.0/features/datatypes/nulls/","version":"v3.0"},"/v3.0/features/datatypes/numerical/":{"title":"Numerical values","summary":"Numerical values <small class=\"text-muted\">page</small>","path":"/v3.0/features/datatypes/numerical/","version":"v3.0"},"/v3.0/features/datatypes/":{"title":"CQL data types to JavaScript types","summary":"CQL data types to JavaScript types <small class=\"text-muted\">page</small>","path":"/v3.0/features/datatypes/","version":"v3.0"},"/v3.0/features/datatypes/tuples/":{"title":"Tuples","summary":"Tuples <small class=\"text-muted\">page</small>","path":"/v3.0/features/datatypes/tuples/","version":"v3.0"},"/v3.0/features/datatypes/udts/":{"title":"User-defined types","summary":"User-defined types <small class=\"text-muted\">page</small>","path":"/v3.0/features/datatypes/udts/","version":"v3.0"},"/v3.0/features/datatypes/uuids/":{"title":"UUID and time-based UUID data types","summary":"UUID and time-based UUID data types <small class=\"text-muted\">page</small>","path":"/v3.0/features/datatypes/uuids/","version":"v3.0"},"/v3.0/features/metadata/":{"title":"Cluster and schema metadata","summary":"Cluster and schema metadata <small class=\"text-muted\">page</small>","path":"/v3.0/features/metadata/","version":"v3.0"},"/v3.0/features/native-protocol/":{"title":"Native protocol","summary":"Native protocol <small class=\"text-muted\">page</small>","path":"/v3.0/features/native-protocol/","version":"v3.0"},"/v3.0/features/paging/":{"title":"Fetching large result sets","summary":"Fetching large result sets <small class=\"text-muted\">page</small>","path":"/v3.0/features/paging/","version":"v3.0"},"/v3.0/features/parameterized-queries/":{"title":"Parameterized queries","summary":"Parameterized queries <small class=\"text-muted\">page</small>","path":"/v3.0/features/parameterized-queries/","version":"v3.0"},"/v3.0/features/query-warnings/":{"title":"Query warnings","summary":"Query warnings <small class=\"text-muted\">page</small>","path":"/v3.0/features/query-warnings/","version":"v3.0"},"/v3.0/features/":{"title":"Features","summary":"Features <small class=\"text-muted\">page</small>","path":"/v3.0/features/","version":"v3.0"},"/v3.0/features/tuning-policies/":{"title":"Tuning policies","summary":"Tuning policies <small class=\"text-muted\">page</small>","path":"/v3.0/features/tuning-policies/","version":"v3.0"},"/v3.0/features/udfs/":{"title":"User-defined functions and aggregates","summary":"User-defined functions and aggregates <small class=\"text-muted\">page</small>","path":"/v3.0/features/udfs/","version":"v3.0"},"/v3.0/faq/":{"title":"Frequently Asked Questions","summary":"Frequently Asked Questions <small class=\"text-muted\">page</small>","path":"/v3.0/faq/","version":"v3.0"},"/v3.0/getting-started/":{"title":"Getting started","summary":"Getting started <small class=\"text-muted\">page</small>","path":"/v3.0/getting-started/","version":"v3.0"},"/v3.0/coding-rules/":{"title":"Three simple rules for coding with the driver","summary":"Three simple rules for coding with the driver <small class=\"text-muted\">page</small>","path":"/v3.0/coding-rules/","version":"v3.0"}})
  app.factory('basePath', basePath)
  app.provider('search', function() {
    function localSearchFactory($http, $timeout, $q, $rootScope, basePath) {
      $rootScope.searchReady = false;

      var fetch = $http.get(basePath + '/json/search-index.json')
                       .then(function(response) {
                         var index = lunr.Index.load(response.data)
                         $rootScope.searchReady = true;
                         return index;
                       });

      // The actual service is a function that takes a query string and
      // returns a promise to the search results
      // (In this case we just resolve the promise immediately as it is not
      // inherently an async process)
      return function(q) {
        return fetch.then(function(index) {
          var results = []
          index.search(q).forEach(function(hit) {
            results.push(hit.ref);
          });
          return results;
        })
      };
    };
    localSearchFactory.$inject = ['$http', '$timeout', '$q', '$rootScope', 'basePath'];

    function webWorkerSearchFactory($q, $rootScope, basePath) {
      $rootScope.searchReady = false;

      var searchIndex = $q.defer();
      var results;
      var worker = new Worker(basePath + '/js/search-worker.js');

      // The worker will send us a message in two situations:
      // - when the index has been built, ready to run a query
      // - when it has completed a search query and the results are available
      worker.onmessage = function(e) {
        switch(e.data.e) {
          case 'ready':
            worker.postMessage({ e: 'load', p: basePath });
            break
          case 'index-ready':
            $rootScope.$apply(function() {
              $rootScope.searchReady = true;
            })
            searchIndex.resolve();
            break;
          case 'query-ready':
            results.resolve(e.data.d);
            break;
        }
      };

      // The actual service is a function that takes a query string and
      // returns a promise to the search results
      return function(q) {

        // We only run the query once the index is ready
        return searchIndex.promise.then(function() {

          results = $q.defer();
          worker.postMessage({ e: 'search', q: q });
          return results.promise;
        });
      };
    };
    webWorkerSearchFactory.$inject = ['$q', '$rootScope', 'basePath'];

    return {
      $get: window.Worker ? webWorkerSearchFactory : localSearchFactory
    };
  })

  app.controller('search', [
    '$scope',
    '$sce',
    '$timeout',
    'search',
    'pages',
    'basePath',
    function($scope, $sce, $timeout, search, pages, basePath) {
      $scope.hasResults = false;
      $scope.results = null;
      $scope.current = null;

      function clear() {
        $scope.hasResults = false;
        $scope.results = null;
        $scope.current = null;
      }

      $scope.search = function(version) {
        if ($scope.q.length >= 2) {
          search($scope.q).then(function(ids) {
            var results = []

            ids.forEach(function(id) {
              var page = pages[id];

              if (page.version == version) {
                results.push(page)
              }
            })

            if (results.length > 0) {
              $scope.hasResults = true;
              $scope.results = results;
              $scope.current = 0;
            } else {
              clear()
            }
          })
        } else {
          clear()
        }
      };

      $scope.basePath = basePath;

      $scope.reset = function() {
        $scope.q = null;
        clear()
      }

      $scope.submit = function() {
        var result = $scope.results[$scope.current]

        if (result) {
          $timeout(function() {
            window.location.href = basePath + result.path;
          })
        }
      }

      $scope.summary = function(item) {
        return $sce.trustAsHtml(item.summary);
      }

      $scope.moveDown = function(e) {
        if ($scope.hasResults && $scope.current < ($scope.results.length - 1)) {
          $scope.current++
          e.stopPropagation()
        }
      }

      $scope.moveUp = function(e) {
        if ($scope.hasResults && $scope.current > 0) {
          $scope.current--
          e.stopPropagation()
        }
      }
    }
  ])

  app.directive('search', [
    '$document',
    'hotkeys',
    function($document, hotkeys) {
      return function(scope, element, attrs) {
        hotkeys.add({
          combo: '/',
          description: 'Search docs...',
          callback: function(event, hotkey) {
            event.preventDefault()
            event.stopPropagation()
            element[0].focus()
          }
        })
      }
    }
  ])

  $(function() {
    $('#content').height(
      Math.max(
        $(".side-nav").height(),
        $('#content').height()
      )
    );

    $('#table-of-contents').on('activate.bs.scrollspy', function() {
      var active = $('#table-of-contents li.active').last().children('a');
      var button = $('#current-section');
      var text   = active.text().trim();

      if (active.length == 0 || text == 'Page Top') {
        button.html('Jump to... <span class="caret"></span><span class="sr-only">Table of Contents</span>')
      } else {
        if (text.length > 30) {
          text = text.slice(0, 30) + '...'
        }
        button.html('Viewing: ' + text + ' <span class="caret"></span><span class="sr-only">Table of Contents</span>')
      }
    })

    // Config ZeroClipboard
    ZeroClipboard.config({
      swfPath: basePath() + '/flash/ZeroClipboard.swf',
      hoverClass: 'btn-clipboard-hover',
      activeClass: 'btn-clipboard-active'
    })

    // Insert copy to clipboard button before .highlight
    $('.highlight').each(function () {
      var btnHtml = '<div class="zero-clipboard"><span class="btn-clipboard">Copy</span></div>'
      $(this).before(btnHtml)
    })

    var zeroClipboard = new ZeroClipboard($('.btn-clipboard'))

    // Handlers for ZeroClipboard

    // Copy to clipboard
    zeroClipboard.on('copy', function (event) {
      var clipboard = event.clipboardData;
      var highlight = $(event.target).parent().nextAll('.highlight').first()
      clipboard.setData('text/plain', highlight.text())
    })

    // Notify copy success and reset tooltip title
    zeroClipboard.on('aftercopy', function (event) {
      $(event.target)
        .attr('title', 'Copied!')
        .tooltip('fixTitle')
        .tooltip('show')
    })

    // Notify copy failure
    zeroClipboard.on('error', function (event) {
      $(event.target)
        .attr('title', 'Flash required')
        .tooltip('fixTitle')
        .tooltip('show')
    })
  })
})(window)
